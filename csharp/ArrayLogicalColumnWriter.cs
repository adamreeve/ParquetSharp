using System;
using System.Linq;
using ParquetSharp.Schema;

namespace ParquetSharp
{
    internal sealed class ArrayLogicalColumnWriter<TPhysical, TLogical, TElement> : LogicalColumnWriter<TElement>
        where TPhysical : unmanaged
    {
        internal ArrayLogicalColumnWriter(ColumnWriter columnWriter, int bufferLength)
            : base(columnWriter, bufferLength)
        {
            _byteBuffer = typeof(TPhysical) == typeof(ByteArray) || typeof(TPhysical) == typeof(FixedLenByteArray)
                ? new ByteBuffer(bufferLength)
                : null;

            // Convert logical values into physical values at the lowest array level
            _converter = (LogicalWrite<TLogical, TPhysical>.Converter) columnWriter
                .LogicalWriteConverterFactory.GetConverter<TLogical, TPhysical>(ColumnDescriptor, _byteBuffer);

            if (typeof(TElement) == typeof(byte[]) || !typeof(TElement).IsArray)
            {
                throw new Exception("unexpected");
            }

            _writer = WriteArray(ArraySchemaNodes!, typeof(TElement), 0, 0, 0);
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

        private Action<Array> WriteArray(Node[] schemaNodes, Type elementType, short repetitionLevel, short nullDefinitionLevel, short firstLeafRepLevel)
        {
            if (elementType.IsArray && elementType != typeof(byte[]))
            {
                if (schemaNodes.Length >= 2 &&
                    schemaNodes[0] is GroupNode { LogicalType: ListLogicalType, Repetition: Repetition.Optional } &&
                    schemaNodes[1] is GroupNode { LogicalType: NoneLogicalType, Repetition: Repetition.Repeated })
                {
                    var containedType = elementType.GetElementType();

                    return WriteArrayIntermediateLevel(
                        schemaNodes.Skip(2).ToArray(),
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

                short leafDefinitionLevel = isOptional ? (short)(nullDefinitionLevel + 1) : nullDefinitionLevel;
                short leafNullDefinitionLevel = isOptional ? nullDefinitionLevel : (short)-1;

                return WriteArrayFinalLevel(repetitionLevel, firstLeafRepLevel, leafDefinitionLevel, leafNullDefinitionLevel);
            }

            throw new Exception("ParquetSharp does not understand the schema used");
        }

        private Action<Array> WriteArrayIntermediateLevel(Node[] schemaNodes, Type elementType, short nullDefinitionLevel, short repetitionLevel, short firstLeafRepLevel)
        {
            var columnWriter = (ColumnWriter<TPhysical>)Source;

            var writer0 = WriteArray(schemaNodes, elementType, (short)(repetitionLevel + 1), (short)(nullDefinitionLevel + 2), firstLeafRepLevel);
            var writer = WriteArray(schemaNodes, elementType, (short)(repetitionLevel + 1), (short)(nullDefinitionLevel + 2), repetitionLevel);

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
                            columnWriter.WriteBatchSpaced(1, new[] { (short)(nullDefinitionLevel + 1) }, new[] { currentLeafRepLevel }, new byte[] { 0 }, 0, new TPhysical[] { });
                        }
                    }
                    else
                    {
                        // Write that this item is null
                        columnWriter.WriteBatchSpaced(1, new[] { nullDefinitionLevel }, new[] { currentLeafRepLevel }, new byte[] { 0 }, 0, new TPhysical[] { });
                    }
                }
            };
        }

        /// <summary>
        /// Write implementation for writing the deepest level array.
        /// </summary>
        private Action<Array> WriteArrayFinalLevel(
            short repetitionLevel, short leafFirstRepLevel,
            short leafDefinitionLevel, short nullDefinitionLevel)
        {
            if (DefLevels == null) throw new InvalidOperationException("DefLevels should not be null.");
            if (RepLevels == null) throw new InvalidOperationException("RepLevels should not be null.");

            var columnWriter = (ColumnWriter<TPhysical>)Source;
            var buffer = (TPhysical[])Buffer;

            return values =>
            {
                ReadOnlySpan<TLogical> valuesSpan = (TLogical[])values;

                var rowsWritten = 0;
                var firstItem = true;

                while (rowsWritten < values.Length)
                {
                    var bufferLength = Math.Min(values.Length - rowsWritten, buffer.Length);

                    _converter(valuesSpan.Slice(rowsWritten, bufferLength), DefLevels, buffer, nullDefinitionLevel);

                    for (int i = 0; i < bufferLength; i++)
                    {
                        RepLevels[i] = repetitionLevel;

                        // If the leaves are required, we have to write the deflevel because the converter won't do this for us.
                        if (nullDefinitionLevel == -1)
                        {
                            DefLevels[i] = leafDefinitionLevel;
                        }
                    }

                    if (firstItem)
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

        private readonly ByteBuffer? _byteBuffer;
        private readonly LogicalWrite<TLogical, TPhysical>.Converter _converter;
        private readonly Action<Array> _writer;
    }

    internal interface IBatchWriter<TElement>
    {
        void WriteBatch(ReadOnlySpan<TElement> values);
    }

}
