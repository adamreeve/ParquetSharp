using System;

namespace ParquetSharp.LogicalBatchWriter
{
    /// <summary>
    /// Writes array values
    /// </summary>
    /// <typeparam name="TItem">The type of the item in the arrays</typeparam>
    /// <typeparam name="TPhysical">The underlying physical type of the column</typeparam>
    internal sealed class ArrayWriter<TItem, TPhysical> : ILogicalBatchWriter<TItem[]>
        where TPhysical : unmanaged
    {
        public ArrayWriter(
            ILogicalBatchWriter<TItem> firstElementWriter,
            ILogicalBatchWriter<TItem> elementWriter,
            BufferedWriter<TPhysical> bufferedWriter,
            bool optionalArrays,
            short definitionLevel,
            short repetitionLevel,
            short firstRepetitionLevel)
        {
            _firstElementWriter = firstElementWriter;
            _elementWriter = elementWriter;
            _bufferedWriter = bufferedWriter;
            _optionalArrays = optionalArrays;
            _definitionLevel = definitionLevel;
            _firstRepetitionLevel = firstRepetitionLevel;
            _repetitionLevel = repetitionLevel;
        }

        public void WriteBatch(ReadOnlySpan<TItem[]> values)
        {
            var nullDefinitionLevel = (short) (_definitionLevel - 1);
            var elementWriter = _firstElementWriter;
            var arrayRepetitionLevel = _firstRepetitionLevel;

            for (var i = 0; i < values.Length; ++i)
            {
                var item = values[i];
                if (item != null)
                {
                    if (item.Length > 0)
                    {
                        elementWriter.WriteBatch(item);
                    }
                    else
                    {
                        // Write zero length array
                        var buffers = _bufferedWriter.GetBuffers(1);
                        buffers.DefLevels[0] = _definitionLevel;
                        buffers.RepLevels[0] = arrayRepetitionLevel;
                        _bufferedWriter.AdvanceBuffers(1, 0);
                    }
                }
                else if (!_optionalArrays)
                {
                    throw new InvalidOperationException("Cannot write a null array value for a required array column");
                }
                else
                {
                    // Write a null array entry
                    var buffers = _bufferedWriter.GetBuffers(1);
                    buffers.DefLevels[0] = nullDefinitionLevel;
                    buffers.RepLevels[0] = arrayRepetitionLevel;
                    _bufferedWriter.AdvanceBuffers(1, 0);
                }

                if (i == 0)
                {
                    elementWriter = _elementWriter;
                    arrayRepetitionLevel = _repetitionLevel;
                }
            }
        }

        private readonly ILogicalBatchWriter<TItem> _firstElementWriter;
        private readonly ILogicalBatchWriter<TItem> _elementWriter;
        private readonly BufferedWriter<TPhysical> _bufferedWriter;
        private readonly short _firstRepetitionLevel;
        private readonly short _repetitionLevel;
        private readonly short _definitionLevel;
        private readonly bool _optionalArrays;
    }
}
