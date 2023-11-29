using System;

namespace ParquetSharp.LogicalBatchWriter
{
    /// <summary>
    /// Buffers data to be written so that for complex nested data structures we can batch writes together
    /// </summary>
    internal sealed class BufferedWriter<TPhysical>
        where TPhysical : unmanaged
    {
        public BufferedWriter(
            ColumnWriter<TPhysical> physicalWriter,
            LogicalStreamBuffers<TPhysical> buffers,
            ByteBuffer? byteBuffer)
        {
            _physicalWriter = physicalWriter;
            _buffers = buffers;
            _byteBuffer = byteBuffer;
        }

        public int MaxBatchLength => _buffers.Length;

        public BuffersView<TPhysical> GetBuffers(int numLevels)
        {
            if (numLevels > _buffers.Length)
            {
                throw new Exception($"Write size ({numLevels}) cannot exceed buffer size ({_buffers.Length})");
            }
            if (_numLevels + numLevels > _buffers.Length)
            {
                Flush();
            }

            return new BuffersView<TPhysical>(
                _buffers.DefLevels == null ? Span<short>.Empty : _buffers.DefLevels.AsSpan(_numLevels),
                _buffers.RepLevels == null ? Span<short>.Empty : _buffers.RepLevels.AsSpan(_numLevels),
                _buffers.Values.AsSpan(_numValues));
        }

        public void AdvanceBuffers(int numLevels, int numValues)
        {
            _numLevels += numLevels;
            _numValues += numValues;
        }

        public void Flush()
        {
            if (_numLevels == 0)
            {
                return;
            }

            _physicalWriter.WriteBatch(
                _numLevels,
                _buffers.DefLevels.AsSpan(),
                _buffers.RepLevels.AsSpan(),
                _buffers.Values.AsSpan());
            _numLevels = 0;
            _numValues = 0;
            _byteBuffer?.Clear();
        }

        private readonly ColumnWriter<TPhysical> _physicalWriter;
        private readonly ByteBuffer? _byteBuffer;
        private readonly LogicalStreamBuffers<TPhysical> _buffers;
        private int _numLevels = 0;
        private int _numValues = 0;
    }

    internal ref struct BuffersView<TPhysical>
    {
        public Span<short> DefLevels { get; }
        public Span<short> RepLevels { get; }
        public Span<TPhysical> Values { get; }

        public BuffersView(Span<short> defLevels, Span<short> repLevels, Span<TPhysical> values)
        {
            DefLevels = defLevels;
            RepLevels = repLevels;
            Values = values;
        }
    }
}
