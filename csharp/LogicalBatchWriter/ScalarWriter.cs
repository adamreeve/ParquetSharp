using System;

namespace ParquetSharp.LogicalBatchWriter
{
    /// <summary>
    /// Writes the lowest level leaf values for a column.
    /// For non-nested data this will be the only writer needed.
    /// </summary>
    internal sealed class ScalarWriter<TLogical, TPhysical> : ILogicalBatchWriter<TLogical>
        where TPhysical : unmanaged
    {
        public ScalarWriter(
            BufferedWriter<TPhysical> bufferedWriter,
            LogicalWrite<TLogical, TPhysical>.Converter converter,
            short definitionLevel,
            short repetitionLevel,
            short firstRepetitionLevel,
            bool optional)
        {
            _bufferedWriter = bufferedWriter;
            _converter = converter;

            _optional = optional;
            _definitionLevel = definitionLevel;
            _repetitionLevel = repetitionLevel;
            _firstRepetitionLevel = firstRepetitionLevel;
        }

        public void WriteBatch(ReadOnlySpan<TLogical> values)
        {
            var rowsWritten = 0;
            var nullDefinitionLevel = (short) (_definitionLevel - 1);
            var firstWrite = true;

            while (rowsWritten < values.Length)
            {
                var bufferLength = Math.Min(values.Length - rowsWritten, _bufferedWriter.MaxBatchLength);

                var buffers = _bufferedWriter.GetBuffers(bufferLength);

                var numValues = _converter(values.Slice(rowsWritten, bufferLength), buffers.DefLevels, buffers.Values, nullDefinitionLevel);

                if (!buffers.RepLevels.IsEmpty)
                {
                    for (var i = 0; i < bufferLength; ++i)
                    {
                        buffers.RepLevels[i] = _repetitionLevel;
                    }
                    if (firstWrite)
                    {
                        buffers.RepLevels[0] = _firstRepetitionLevel;
                    }
                }

                if (!_optional && !buffers.DefLevels.IsEmpty)
                {
                    // The converter doesn't handle writing definition levels for non-optional values, so write these now
                    for (var i = 0; i < bufferLength; ++i)
                    {
                        buffers.DefLevels[i] = _definitionLevel;
                    }
                }

                _bufferedWriter.AdvanceBuffers(bufferLength, numValues);
                rowsWritten += bufferLength;

                firstWrite = false;
            }
        }

        private readonly LogicalWrite<TLogical, TPhysical>.Converter _converter;
        private readonly BufferedWriter<TPhysical> _bufferedWriter;
        private readonly short _definitionLevel;
        private readonly short _repetitionLevel;
        private readonly short _firstRepetitionLevel;
        private readonly bool _optional;
    }
}
