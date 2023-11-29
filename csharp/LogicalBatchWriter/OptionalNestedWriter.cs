using System;

namespace ParquetSharp.LogicalBatchWriter
{
    /// <summary>
    /// Writes optional nested values by unwrapping the nesting
    /// </summary>
    internal sealed class OptionalNestedWriter<TItem, TPhysical> : ILogicalBatchWriter<Nested<TItem>?>
        where TPhysical : unmanaged
    {
        public OptionalNestedWriter(
            ILogicalBatchWriter<TItem> firstInnerWriter,
            ILogicalBatchWriter<TItem> innerWriter,
            BufferedWriter<TPhysical> bufferedWriter,
            short definitionLevel,
            short repetitionLevel,
            short firstRepetitionLevel)
        {
            _firstInnerWriter = firstInnerWriter;
            _innerWriter = innerWriter;
            _bufferedWriter = bufferedWriter;
            _definitionLevel = definitionLevel;
            _repetitionLevel = repetitionLevel;
            _firstRepetitionLevel = firstRepetitionLevel;
            _buffer = new TItem[_bufferedWriter.MaxBatchLength];
        }

        public void WriteBatch(ReadOnlySpan<Nested<TItem>?> values)
        {
            var nullDefinitionLevel = (short) (_definitionLevel - 1);
            var writer = _firstInnerWriter;
            var offset = 0;

            while (offset < values.Length)
            {
                // Get non-null values and pass them through to the inner writer
                var maxSpanSize = Math.Min(values.Length - offset, _buffer.Length);
                var nonNullSpanSize = maxSpanSize;
                for (var i = 0; i < maxSpanSize; ++i)
                {
                    var value = values[offset + i];
                    if (value == null)
                    {
                        nonNullSpanSize = i;
                        break;
                    }
                    _buffer[i] = value.Value.Value;
                }

                if (nonNullSpanSize > 0)
                {
                    writer.WriteBatch(_buffer.AsSpan(0, nonNullSpanSize));
                    offset += nonNullSpanSize;
                }

                // Count any null values
                maxSpanSize = Math.Min(values.Length - offset, _buffer.Length);
                var nullSpanSize = maxSpanSize;
                for (var i = 0; i < maxSpanSize; ++i)
                {
                    var value = values[offset + i];
                    if (value != null)
                    {
                        nullSpanSize = i;
                        break;
                    }
                }

                if (nullSpanSize > 0)
                {
                    var buffers = _bufferedWriter.GetBuffers(nullSpanSize);
                    // Write a batch of null values
                    for (var i = 0; i < nullSpanSize; ++i)
                    {
                        buffers.DefLevels[i] = nullDefinitionLevel;
                    }

                    if (!buffers.RepLevels.IsEmpty)
                    {
                        for (var i = 0; i < nullSpanSize; ++i)
                        {
                            buffers.RepLevels[i] = _repetitionLevel;
                        }
                        if (offset == 0)
                        {
                            buffers.RepLevels[0] = _firstRepetitionLevel;
                        }
                    }

                    _bufferedWriter.AdvanceBuffers(nullSpanSize, 0);

                    offset += nullSpanSize;
                }

                writer = _innerWriter;
            }
        }

        private readonly ILogicalBatchWriter<TItem> _firstInnerWriter;
        private readonly ILogicalBatchWriter<TItem> _innerWriter;
        private readonly BufferedWriter<TPhysical> _bufferedWriter;
        private readonly short _definitionLevel;
        private readonly short _repetitionLevel;
        private readonly short _firstRepetitionLevel;
        private readonly TItem[] _buffer;
    }
}
