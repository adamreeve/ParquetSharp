using System;
using System.Collections.Generic;
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

            _bufferedReader = new BufferedReader<TPhysical>(Source, (TPhysical[]) Buffer, DefLevels, RepLevels);
            _converter = (LogicalRead<TLogical, TPhysical>.Converter) converterFactory
                .GetConverter<TLogical, TPhysical>(ColumnDescriptor, columnReader.ColumnChunkMetaData);

            ArraySchemaNodes = GetSchemaNode(ColumnDescriptor.SchemaNode).ToArray();
        }

        public override int ReadBatch(Span<TElement> destination)
        {
            var result = (Span<TElement>) (TElement[]) ReadArray(ArraySchemaNodes, typeof(TElement), destination.Length, 0, 0);

            result.CopyTo(destination);

            return result.Length;
        }

        private Array ReadArray(ReadOnlySpan<Node> schemaNodes, Type elementType, int numArrayEntriesToRead, int repetitionLevel, int nullDefinitionLevel)
        {
            if (elementType.IsArray && elementType != typeof(byte[]))
            {
                if (schemaNodes.Length >= 2 &&
                    schemaNodes[0] is GroupNode { LogicalType: ListLogicalType, Repetition: Repetition.Optional } &&
                    schemaNodes[1] is GroupNode { LogicalType: NoneLogicalType, Repetition: Repetition.Repeated })
                {
                    return ReadArrayIntermediateLevel(schemaNodes, elementType, numArrayEntriesToRead, 
                        (short)repetitionLevel, (short)nullDefinitionLevel);
                }

                throw new Exception("elementType is an array but schema does not match the expected layout");
            }

            if (schemaNodes.Length == 1)
            {
                bool optional = schemaNodes[0].Repetition == Repetition.Optional;

                return ReadArrayLeafLevel(optional, (short)repetitionLevel, (short)nullDefinitionLevel);
            }

            throw new Exception("ParquetSharp does not understand the schema used");
        }

        private Array ReadArrayIntermediateLevel(ReadOnlySpan<Node> schemaNodes, 
            Type elementType, int numArrayEntriesToRead, short repetitionLevel, short nullDefinitionLevel)
        {
            var acc = new List<Array?>();

            while (numArrayEntriesToRead == -1 || acc.Count < numArrayEntriesToRead)
            {
                var defn = _bufferedReader.GetCurrentDefinition();

                Array? newItem = null;

                if (defn.DefLevel >= nullDefinitionLevel + 2)
                {
                    newItem = ReadArray(schemaNodes.Slice(2), elementType.GetElementType(), -1, repetitionLevel + 1, nullDefinitionLevel + 2);
                }
                else
                {
                    if (defn.DefLevel == nullDefinitionLevel + 1)
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
        }

        private Array ReadArrayLeafLevel(bool optional, short repetitionLevel, short nullDefinitionLevel)
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
            _converter(values.ToArray(), defnLevel.ToArray(), dest, nullDefinitionLevel);
            return dest;
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

        private readonly BufferedReader<TPhysical> _bufferedReader;
        private readonly LogicalRead<TLogical, TPhysical>.Converter _converter;
        private readonly Node[] ArraySchemaNodes;
    }
}
