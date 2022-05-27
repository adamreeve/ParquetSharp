using System;
using System.Linq;
using ParquetSharp.Schema;

namespace ParquetSharp
{
    internal sealed class CompoundLogicalColumnWriter<TPhysical, TLogical, TElement> : LogicalColumnWriter<TElement>
        where TPhysical : unmanaged
    {
        internal CompoundLogicalColumnWriter(ColumnWriter columnWriter, int bufferLength)
            : base(columnWriter, bufferLength)
        {
            _byteBuffer = typeof(TPhysical) == typeof(ByteArray) || typeof(TPhysical) == typeof(FixedLenByteArray)
                ? new ByteBuffer(bufferLength)
                : null;

            // Convert logical values into physical values at the lowest array level
            _converter = (LogicalWrite<TLogical, TPhysical>.Converter) columnWriter
                .LogicalWriteConverterFactory.GetConverter<TLogical, TPhysical>(ColumnDescriptor, _byteBuffer);

            //if (typeof(TElement) == typeof(byte[]) || !typeof(TElement).IsArray)
            //{
            //    throw new Exception("unexpected");
            //}

            if (RepLevelsRequired(typeof(TElement)) && RepLevels == null)
            {
                throw new Exception("RepLevels are required but missing");
            }

            _writer = MakeWriter(GetSchemaNode(ColumnDescriptor.SchemaNode).ToArray(), typeof(TElement), 0, 0, 0, false);
        }

        public override void Dispose()
        {
            _byteBuffer?.Dispose();

            base.Dispose();
        }

        public override void WriteBatch(ReadOnlySpan<TElement> values)
        {
            _writer(values.ToArray());
        }

        private Action<Array> MakeWriter(Node[] schemaNodes, Type elementType, short repetitionLevel,
            short nullDefinitionLevel, short firstLeafRepLevel, bool singleItem)
        {
            if (IsNullable(elementType, out var innerNullable) && IsNested(innerNullable, out var innerNested))
            {
                if (schemaNodes.Length >= 1 &&
                    schemaNodes[0] is GroupNode {LogicalType: NoneLogicalType, Repetition: Repetition.Optional})
                {
                    return MakeGenericWriter(nameof(MakeOptionalNestedWriter), innerNested, schemaNodes.Skip(1).ToArray(),
                        repetitionLevel, nullDefinitionLevel, firstLeafRepLevel);
                }

                throw new Exception("elementType is nested (optional) but schema does not match expected layout");
            }

            if (IsNested(elementType, out var innerNestedRequired))
            {
                if (schemaNodes.Length >= 1 &&
                    schemaNodes[0] is GroupNode {LogicalType: NoneLogicalType, Repetition: Repetition.Required})
                {
                    return MakeGenericWriter(nameof(MakeNestedWriter), innerNestedRequired, schemaNodes.Skip(1).ToArray(),
                        repetitionLevel, nullDefinitionLevel, firstLeafRepLevel);
                }

                throw new Exception("elementType is nested (required) but schema does not match expected layout");
            }

            if (elementType.IsArray && elementType != typeof(byte[]))
            {
                if (schemaNodes.Length >= 2 &&
                    schemaNodes[0] is GroupNode {LogicalType: ListLogicalType or MapLogicalType, Repetition: Repetition.Optional or Repetition.Required} &&
                    schemaNodes[1] is GroupNode {LogicalType: NoneLogicalType, Repetition: Repetition.Repeated})
                {
                    var containedType = elementType.GetElementType();

                    return MakeArrayWriter(
                        schemaNodes,
                        containedType,
                        nullDefinitionLevel,
                        repetitionLevel,
                        firstLeafRepLevel
                    );
                }

                throw new Exception("elementType is an array but schema does not match the expected layout");
            }

            if (schemaNodes.Length == 1)
            {
                bool isOptional = schemaNodes[0].Repetition == Repetition.Optional;

                short leafDefinitionLevel = isOptional ? (short) (nullDefinitionLevel + 1) : nullDefinitionLevel;
                short leafNullDefinitionLevel = isOptional ? nullDefinitionLevel : (short) -1;

                if (singleItem)
                {
                    return MakeLeafWriterSingle(repetitionLevel, firstLeafRepLevel, leafDefinitionLevel, leafNullDefinitionLevel);
                }
                else
                {
                    return MakeLeafWriter(repetitionLevel, firstLeafRepLevel, leafDefinitionLevel, leafNullDefinitionLevel);
                }
            }

            throw new Exception("ParquetSharp does not understand the schema used");
        }

        private Action<Array> MakeArrayWriter(Node[] schemaNodes, Type elementType, short nullDefinitionLevel, short repetitionLevel, short firstLeafRepLevel)
        {
            if (schemaNodes.Length < 3)
            {
                throw new ArgumentException("Need at least 3 nodes for a map or array logical type column");
            }

            bool isArrayOptional = schemaNodes[0].Repetition == Repetition.Optional;

            var innerNullDefinitionLevel = (short) (nullDefinitionLevel + (isArrayOptional ? 2 : 1));

            var columnWriter = (ColumnWriter<TPhysical>) Source;

            var writer0 = MakeWriter(schemaNodes.Skip(2).ToArray(), elementType, (short) (repetitionLevel + 1),
                innerNullDefinitionLevel, firstLeafRepLevel, false);
            var writer = MakeWriter(schemaNodes.Skip(2).ToArray(), elementType, (short) (repetitionLevel + 1),
                innerNullDefinitionLevel, repetitionLevel, false);

            return values =>
            {
                for (var i = 0; i < values.Length; i++)
                {
                    var currentLeafRepLevel = i > 0 ? repetitionLevel : firstLeafRepLevel;

                    var item = values.GetValue(i);

                    if (item != null)
                    {
                        if (!(item is Array a))
                        {
                            throw new Exception("non-array encountered at non-leaf level");
                        }
                        if (a.Length > 0)
                        {
                            // We have a positive length array, call the top level array writer on its values
                            (i > 0 ? writer : writer0)(a);
                        }
                        else
                        {
                            // Write that we have a zero length array
                            columnWriter.WriteBatchSpaced(1, new[] {(short) (nullDefinitionLevel + (isArrayOptional ? 1 : 0))},
                                new[] {currentLeafRepLevel}, new byte[] {0}, 0, new TPhysical[] { });
                        }
                    }
                    else
                    {
                        if (isArrayOptional)
                        {
                            // Write that this item is null
                            columnWriter.WriteBatchSpaced(1, new[] {nullDefinitionLevel}, new[] {currentLeafRepLevel}, new byte[] {0}, 0, new TPhysical[] { });
                        }
                        else
                        {
                            throw new InvalidOperationException("Cannot write a null array value for a required array column");
                        }
                    }
                }
            };
        }

        /// <summary>
        /// Write implementation for writing the deepest level array.
        /// </summary>
        private Action<Array> MakeLeafWriter(
            short repetitionLevel, short leafFirstRepLevel,
            short leafDefinitionLevel, short nullDefinitionLevel)
        {
            var columnWriter = (ColumnWriter<TPhysical>) Source;
            var buffer = (TPhysical[]) Buffer;

            return values =>
            {
                ReadOnlySpan<TLogical> valuesSpan = (TLogical[]) values;

                var rowsWritten = 0;
                var firstItem = true;

                while (rowsWritten < values.Length)
                {
                    var bufferLength = Math.Min(values.Length - rowsWritten, buffer.Length);

                    _converter(valuesSpan.Slice(rowsWritten, bufferLength), DefLevels, buffer, nullDefinitionLevel);

                    for (int i = 0; i < bufferLength; i++)
                    {
                        if (RepLevels != null)
                        {
                            RepLevels[i] = repetitionLevel;
                        }

                        // If the leaves are required, we have to write the deflevel because the converter won't do this for us.
                        if (nullDefinitionLevel == -1 && DefLevels != null)
                        {
                            DefLevels[i] = leafDefinitionLevel;
                        }
                    }

                    if (firstItem && RepLevels != null)
                    {
                        RepLevels[0] = leafFirstRepLevel;
                    }

                    columnWriter.WriteBatch(bufferLength, DefLevels, RepLevels, buffer);
                    rowsWritten += bufferLength;
                    firstItem = false;

                    _byteBuffer?.Clear();
                }
            };
        }

        private Action<Array> MakeLeafWriterSingle(
            short repetitionLevel, short leafFirstRepLevel,
            short leafDefinitionLevel, short nullDefinitionLevel)
        {
            var columnWriter = (ColumnWriter<TPhysical>) Source;
            var buffer = (TPhysical[]) Buffer;

            return values =>
            {
                ReadOnlySpan<TLogical> valuesSpan = (TLogical[]) values;

                if (valuesSpan.Length != 1)
                {
                    throw new Exception("expected only single item");
                }

                _converter(valuesSpan, DefLevels, buffer, nullDefinitionLevel);

                // If the leaves are required, we have to write the deflevel because the converter won't do this for us.
                if (DefLevels != null && nullDefinitionLevel == -1)
                {
                    DefLevels[0] = leafDefinitionLevel;
                }

                columnWriter.WriteBatch(1, DefLevels, RepLevels, buffer);

                _byteBuffer?.Clear();
            };
        }

        // Writes a Nested<TInner>?[] array
        private Action<Array> MakeOptionalNestedWriter<TInner>(Node[] schemaNodes,
            short repetitionLevel, short nullDefinitionLevel, short firstLeafRepLevel)
        {
            var columnWriter = (ColumnWriter<TPhysical>) Source;

            var writer0 = MakeWriter(schemaNodes, typeof(TInner), repetitionLevel,
                (short) (nullDefinitionLevel + 1), firstLeafRepLevel, true);
            var writer = MakeWriter(schemaNodes, typeof(TInner), repetitionLevel,
                (short) (nullDefinitionLevel + 1), repetitionLevel, true);

            return array =>
            {
                var items = (Nested<TInner>?[]) array;

                for (var i = 0; i < items.Length; i++)
                {
                    var item = items[i];

                    if (item.HasValue)
                    {
                        // We have a positive length array, call the top level array writer on its values
                        (i > 0 ? writer : writer0)(new[] {item.Value.Value});
                    }
                    else
                    {
                        // Write that this item is null
                        columnWriter.WriteBatchSpaced(1, new[] {nullDefinitionLevel},
                            new[] {repetitionLevel}, new byte[] {0}, 0, new TPhysical[] { });
                    }
                }
            };
        }

        // Writes a Nested<TInner>[] array
        // TODO: This is essentially a no-op and could be handled at the converter level?
        private Action<Array> MakeNestedWriter<TInner>(Node[] schemaNodes,
            short repetitionLevel, short nullDefinitionLevel, short firstLeafRepLevel)
        {
            var columnWriter = (ColumnWriter<TPhysical>) Source;

            var writer0 = MakeWriter(schemaNodes, typeof(TInner), repetitionLevel,
                nullDefinitionLevel, firstLeafRepLevel, true);
            var writer = MakeWriter(schemaNodes, typeof(TInner), repetitionLevel,
                nullDefinitionLevel, repetitionLevel, true);

            return array =>
            {
                var items = (Nested<TInner>[]) array;

                for (var i = 0; i < items.Length; i++)
                {
                    (i > 0 ? writer : writer0)(new[] {items[i].Value});
                }
            };
        }

        private Action<Array> MakeGenericWriter(string name, Type type, Node[] schemaNodes,
            short repetitionLevel, short nullDefinitionLevel, short firstLeafRepLevel)
        {
            var iface = typeof(CompoundLogicalColumnWriter<TPhysical, TLogical, TElement>);
            var genericMethod = iface.GetMethod(name, System.Reflection.BindingFlags.Instance | System.Reflection.BindingFlags.NonPublic);
            return (Action<Array>) genericMethod.MakeGenericMethod(type).Invoke(this, new object[]
            {
                schemaNodes, repetitionLevel, nullDefinitionLevel, firstLeafRepLevel
            });
        }

        private readonly ByteBuffer? _byteBuffer;
        private readonly LogicalWrite<TLogical, TPhysical>.Converter _converter;
        private readonly Action<Array> _writer;
    }
}
