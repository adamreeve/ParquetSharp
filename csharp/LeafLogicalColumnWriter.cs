using System;

namespace ParquetSharp
{
    internal sealed class LeafLogicalColumnWriter<TPhysical, TLogical, TElement> : LogicalColumnWriter<TElement>
        where TPhysical : unmanaged
    {
        public LeafLogicalColumnWriter(ColumnWriter columnWriter, int bufferLength) : base(columnWriter, bufferLength)
        {
            _byteBuffer = typeof(TPhysical) == typeof(ByteArray) || typeof(TPhysical) == typeof(FixedLenByteArray)
                ? new ByteBuffer(bufferLength)
                : null;

            // Convert logical values into physical values at the lowest array level
            _converter = (LogicalWrite<TLogical, TPhysical>.Converter) columnWriter
                .LogicalWriteConverterFactory
                .GetConverter<TLogical, TPhysical>(ColumnDescriptor, _byteBuffer);
        }

        public override void Dispose()
        {
            _byteBuffer?.Dispose();

            base.Dispose();
        }

        public override void WriteBatch(ReadOnlySpan<TElement> values)
        {
            WriteBatchSimple(values);
        }

        /// <summary>
        /// Fast implementation when a column contains only flat primitive values.
        /// </summary>
        private void WriteBatchSimple<TTLogical>(ReadOnlySpan<TTLogical> values)
        {
            if (typeof(TTLogical) != typeof(TLogical)) throw new ArgumentException("generic logical type should never be different");

            var rowsWritten = 0;
            var nullLevel = DefLevels == null ? (short)-1 : (short)0;
            var columnWriter = (ColumnWriter<TPhysical>)Source;
            var buffer = (TPhysical[])Buffer;

            var converter = _converter as LogicalWrite<TTLogical, TPhysical>.Converter;
            if (converter == null)
            {
                throw new InvalidCastException("failed to cast writer convert");
            }

            while (rowsWritten < values.Length)
            {
                var bufferLength = Math.Min(values.Length - rowsWritten, buffer.Length);

                converter(values.Slice(rowsWritten, bufferLength), DefLevels, buffer, nullLevel);
                columnWriter.WriteBatch(bufferLength, DefLevels, RepLevels, buffer);
                rowsWritten += bufferLength;

                _byteBuffer?.Clear();
            }
        }

        private readonly ByteBuffer? _byteBuffer;
        private readonly LogicalWrite<TLogical, TPhysical>.Converter _converter;
    }
}
