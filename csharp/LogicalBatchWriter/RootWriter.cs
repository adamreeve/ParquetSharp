using System;

namespace ParquetSharp.LogicalBatchWriter
{
    internal sealed class RootWriter<TLogical, TPhysical> : ILogicalBatchWriter<TLogical>
      where TPhysical: unmanaged
    {
        public RootWriter(
            ILogicalBatchWriter<TLogical> innerWriter,
            BufferedWriter<TPhysical> bufferedWriter)
        {
            _innerWriter = innerWriter;
            _bufferedWriter = bufferedWriter;
        }

        public void WriteBatch(ReadOnlySpan<TLogical> values)
        {
            _innerWriter.WriteBatch(values);
            _bufferedWriter.Flush();
        }

        private readonly ILogicalBatchWriter<TLogical> _innerWriter;
        private readonly BufferedWriter<TPhysical> _bufferedWriter;
    }
}
