using System;
using System.Collections.Generic;
using System.Linq;
using ParquetSharp.Schema;

namespace ParquetSharp
{
    internal sealed class CompoundLogicalColumnReader<TPhysical, TLogical, TElement> : LogicalColumnReader<TElement>
        where TPhysical : unmanaged
    {
        internal CompoundLogicalColumnReader(ColumnReader columnReader, int bufferLength)
            : base(columnReader, bufferLength)
        {
            var converterFactory = columnReader.LogicalReadConverterFactory;

            if (RepLevelsRequired(typeof(TElement)) && RepLevels == null)
            {
                throw new Exception("RepLevels are required but missing");
            }

            _bufferedReader = new BufferedReader<TPhysical>(Source, (TPhysical[]) Buffer, DefLevels, RepLevels);
            _converter = (LogicalRead<TLogical, TPhysical>.Converter) converterFactory
                .GetConverter<TLogical, TPhysical>(ColumnDescriptor, columnReader.ColumnChunkMetaData);

            _reader = MakeReader(GetSchemaNode(ColumnDescriptor.SchemaNode).ToArray(), typeof(TElement), 0, 0, false);
        }

        public override int ReadBatch(Span<TElement> destination)
        {
            var result = (Span<TElement>) (TElement[]) _reader(destination.Length);

            result.CopyTo(destination);

            return result.Length;
        }

        private Func<int, Array> MakeReader(Node[] schemaNodes, Type elementType, int repetitionLevel, int nullDefinitionLevel, bool wantSingleItem)
        {
            if (IsNullable(elementType, out var innerNullable) && IsNested(innerNullable, out var innerNested))
            {
                if (schemaNodes.Length >= 1 &&
                    schemaNodes[0] is GroupNode {LogicalType: NoneLogicalType, Repetition: Repetition.Optional})
                {
                    return MakeGenericReader(nameof(MakeNestedOptionalReader), innerNested, schemaNodes.Skip(1).ToArray(),
                        repetitionLevel, nullDefinitionLevel);
                }

                throw new Exception("elementType is nested (optional) but schema does not match expected layout");
            }

            if (IsNested(elementType, out var innerNestedRequired))
            {
                if (schemaNodes.Length >= 1 &&
                    schemaNodes[0] is GroupNode {LogicalType: NoneLogicalType, Repetition: Repetition.Required})
                {
                    return MakeGenericReader(nameof(MakeNestedReader), innerNestedRequired, schemaNodes.Skip(1).ToArray(),
                        repetitionLevel, nullDefinitionLevel);
                }

                throw new Exception("elementType is nested (required) but schema does not match expected layout");
            }

            if (elementType.IsArray && elementType != typeof(byte[]))
            {
                if (schemaNodes.Length >= 2 &&
                    schemaNodes[0] is GroupNode {LogicalType: ListLogicalType or MapLogicalType, Repetition: Repetition.Optional or Repetition.Required} &&
                    schemaNodes[1] is GroupNode {LogicalType: NoneLogicalType, Repetition: Repetition.Repeated})
                {
                    return MakeArrayReader(
                        schemaNodes,
                        elementType,
                        (short) repetitionLevel,
                        (short) nullDefinitionLevel
                    );
                }

                throw new Exception("elementType is an array but schema does not match the expected layout");
            }

            if (schemaNodes.Length == 1)
            {
                bool optional = schemaNodes[0].Repetition == Repetition.Optional;

                if (wantSingleItem)
                {
                    var leafReader = MakeLeafReaderSingle(optional, (short) repetitionLevel, (short) nullDefinitionLevel);

                    return numElementsToRead =>
                    {
                        if (numElementsToRead != 1)
                        {
                            throw new Exception("numElementsToRead should be 1");
                        }
                        return leafReader();
                    };
                }
                else
                {
                    var leafReader = MakeLeafReader(optional, (short) repetitionLevel, (short) nullDefinitionLevel);

                    return numElementsToRead =>
                    {
                        if (numElementsToRead != -1)
                        {
                            throw new Exception("numElementsToRead should be -1");
                        }
                        return leafReader();
                    };
                }
            }

            throw new Exception("ParquetSharp does not understand the schema used");
        }

        private Func<int, Array> MakeNestedOptionalReader<TInner>(Node[] schemaNodes, int repetitionLevel, int nullDefinitionLevel)
        {
            var innerReader = MakeReader(schemaNodes, typeof(TInner), repetitionLevel, nullDefinitionLevel + 1, true);

            return numArrayEntriesToRead =>
            {
                var acc = new List<Nested<TInner>?>();

                while (numArrayEntriesToRead == -1 || acc.Count < numArrayEntriesToRead)
                {
                    var defn = _bufferedReader.GetCurrentDefinition();

                    Nested<TInner>? newItem = null;

                    if (defn.DefLevel > nullDefinitionLevel)
                    {
                        newItem = new Nested<TInner>(((TInner[]) innerReader(1))[0]);
                    }
                    else
                    {
                        _bufferedReader.NextDefinition();
                    }

                    acc.Add(newItem);

                    if (_bufferedReader.IsEofDefinition || (RepLevels != null && _bufferedReader.GetCurrentDefinition().RepLevel < repetitionLevel))
                    {
                        break;
                    }
                }

                return acc.ToArray();
            };
        }

        // Reads a Nested<TInner> array
        // TODO: This is essentially a no-op and could be handled by the converter?
        private Func<int, Array> MakeNestedReader<TInner>(Node[] schemaNodes, int repetitionLevel, int nullDefinitionLevel)
        {
            var innerReader = MakeReader(schemaNodes, typeof(TInner), repetitionLevel, nullDefinitionLevel, true);

            return numArrayEntriesToRead =>
            {
                var acc = new List<Nested<TInner>>();

                while (numArrayEntriesToRead == -1 || acc.Count < numArrayEntriesToRead)
                {
                    var newItem = new Nested<TInner>(((TInner[]) innerReader(1))[0]);

                    acc.Add(newItem);

                    if (_bufferedReader.IsEofDefinition || (RepLevels != null && _bufferedReader.GetCurrentDefinition().RepLevel < repetitionLevel))
                    {
                        break;
                    }
                }

                return acc.ToArray();
            };
        }

        private Func<int, Array> MakeArrayReader(Node[] schemaNodes,
            Type elementType, short repetitionLevel, short nullDefinitionLevel)
        {
            if (schemaNodes.Length < 3)
            {
                throw new ArgumentException("Need at least 3 nodes for a map or array logical type column");
            }

            bool isArrayOptional = schemaNodes[0].Repetition == Repetition.Optional;

            var innerNullDefinitionLevel = nullDefinitionLevel + (isArrayOptional ? 2 : 1);

            var innerReader = MakeReader(
                schemaNodes.Skip(2).ToArray(),
                elementType.GetElementType(),
                repetitionLevel + 1,
                innerNullDefinitionLevel,
                false);

            return numArrayEntriesToRead =>
            {
                var acc = new List<Array?>();

                while (numArrayEntriesToRead == -1 || acc.Count < numArrayEntriesToRead)
                {
                    var defn = _bufferedReader.GetCurrentDefinition();

                    Array? newItem = null;

                    if (defn.DefLevel >= innerNullDefinitionLevel)
                    {
                        newItem = innerReader(-1);
                    }
                    else
                    {
                        if (!isArrayOptional || defn.DefLevel == nullDefinitionLevel + 1)
                        {
                            newItem = CreateEmptyArray(elementType);
                        }
                        _bufferedReader.NextDefinition();
                    }

                    acc.Add(newItem);

                    if (_bufferedReader.IsEofDefinition || _bufferedReader.GetCurrentDefinition().RepLevel < repetitionLevel)
                    {
                        break;
                    }
                }

                return ListToArray(acc, elementType);
            };
        }

        private Func<Array> MakeLeafReader(bool optional, short repetitionLevel, short nullDefinitionLevel)
        {
            var definedLevel = (short) (nullDefinitionLevel + (optional ? 1 : 0));

            return () =>
            {
                var defnLevel = new List<short>();
                var values = new List<TPhysical>();
                var firstValue = true;

                while (!_bufferedReader.IsEofDefinition)
                {
                    var defn = _bufferedReader.GetCurrentDefinition();

                    if (!firstValue && defn.RepLevel < repetitionLevel)
                    {
                        break;
                    }

                    if (defn.DefLevel < nullDefinitionLevel)
                    {
                        throw new Exception("Invalid input stream.");
                    }

                    if (defn.DefLevel > nullDefinitionLevel || !optional)
                    {
                        values.Add(_bufferedReader.ReadValue());
                    }

                    defnLevel.Add(defn.DefLevel);

                    _bufferedReader.NextDefinition();
                    firstValue = false;
                }

                var dest = new TLogical[defnLevel.Count];
                _converter(values.ToArray(), defnLevel.ToArray(), dest, definedLevel);
                return dest;
            };
        }

        private Func<Array> MakeLeafReaderSingle(bool optional, short repetitionLevel, short nullDefinitionLevel)
        {
            var definedLevel = (short) (nullDefinitionLevel + (optional ? 1 : 0));

            return () =>
            {
                var values = new List<TPhysical>();

                var defn = _bufferedReader.GetCurrentDefinition();

                if (defn.DefLevel < nullDefinitionLevel)
                {
                    throw new Exception("Invalid input stream.");
                }

                if (defn.DefLevel > nullDefinitionLevel || !optional)
                {
                    values.Add(_bufferedReader.ReadValue());
                }

                var defnLevel = new[] {defn.DefLevel};

                _bufferedReader.NextDefinition();

                var dest = new TLogical[defnLevel.Length];
                _converter(values.ToArray(), defnLevel, dest, definedLevel);
                return dest;
            };
        }

        private static Array ListToArray(List<Array?> list, Type elementType)
        {
            var result = Array.CreateInstance(elementType, list.Count);

            for (int i = 0; i != list.Count; ++i)
            {
                result.SetValue(list[i], i);
            }

            return result;
        }

        private static Array CreateEmptyArray(Type elementType)
        {
            if (elementType.IsArray)
            {
                return Array.CreateInstance(elementType.GetElementType() ?? throw new InvalidOperationException(), 0);
            }

            throw new ArgumentException(nameof(elementType));
        }

        private Func<int, Array> MakeGenericReader(string name, Type type, Node[] schemaNodes, int repetitionLevel, int nullDefinitionLevel)
        {
            var iface = typeof(CompoundLogicalColumnReader<TPhysical, TLogical, TElement>);
            var genericMethod = iface.GetMethod(name, System.Reflection.BindingFlags.Instance | System.Reflection.BindingFlags.NonPublic);
            return (Func<int, Array>) genericMethod.MakeGenericMethod(type).Invoke(this, new object[]
            {
                schemaNodes, repetitionLevel, nullDefinitionLevel
            });
        }

        private readonly BufferedReader<TPhysical> _bufferedReader;
        private readonly LogicalRead<TLogical, TPhysical>.Converter _converter;
        private readonly Func<int, Array> _reader;
    }
}
