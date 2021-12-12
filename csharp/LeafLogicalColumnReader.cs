using System;

namespace ParquetSharp
{
    internal sealed class LeafLogicalColumnReader<TPhysical, TLogical, TElement> : LogicalColumnReader<TElement>
        where TPhysical : unmanaged
    {
        internal LeafLogicalColumnReader(ColumnReader columnReader, int bufferLength)
            : base(columnReader, bufferLength)
        {
            var converterFactory = columnReader.LogicalReadConverterFactory;

            _directReader = (LogicalRead<TLogical, TPhysical>.DirectReader?) converterFactory
                .GetDirectReader<TLogical, TPhysical>();
            _converter = (LogicalRead<TLogical, TPhysical>.Converter) converterFactory
                .GetConverter<TLogical, TPhysical>(ColumnDescriptor, columnReader.ColumnChunkMetaData);
        }

        public override int ReadBatch(Span<TElement> destination)
        {
            // Otherwise deal with flat values.
            return ReadBatchSimple(
                destination,
                _directReader as LogicalRead<TElement, TPhysical>.DirectReader,
                (_converter as LogicalRead<TElement, TPhysical>.Converter)!);
        }

        /// <summary>
        /// Fast implementation when a column contains only flat primitive values.
        /// </summary>
        private int ReadBatchSimple<TTLogical>(
            Span<TTLogical> destination,
            LogicalRead<TTLogical, TPhysical>.DirectReader? directReader,
            LogicalRead<TTLogical, TPhysical>.Converter converter)
        {
            if (typeof(TTLogical) != typeof(TLogical)) throw new ArgumentException("generic logical type should never be different");
            if (directReader != null && DefLevels != null) throw new ArgumentException("direct reader cannot be provided if type is optional");
            if (converter == null) throw new ArgumentNullException(nameof(converter));

            var columnReader = (ColumnReader<TPhysical>)Source;
            var rowsRead = 0;

            // Fast path for logical types that directly map to the physical type in memory.
            if (directReader != null && HasNext)
            {
                while (rowsRead < destination.Length && HasNext)
                {
                    var toRead = destination.Length - rowsRead;
                    var read = checked((int)directReader(columnReader, destination.Slice(rowsRead, toRead)));
                    rowsRead += read;
                }

                return rowsRead;
            }

            // Normal path for logical types that need to be converted from the physical types.
            var definedLevel = DefLevels == null ? (short)0 : (short)1;
            var buffer = (TPhysical[])Buffer;

            while (rowsRead < destination.Length && HasNext)
            {
                var toRead = Math.Min(destination.Length - rowsRead, Buffer.Length);
                var read = checked((int)columnReader.ReadBatch(toRead, DefLevels, RepLevels, buffer, out var valuesRead));
                converter(buffer.AsSpan(0, checked((int)valuesRead)), DefLevels, destination.Slice(rowsRead, read), definedLevel);
                rowsRead += read;
            }

            return rowsRead;
        }

        private readonly LogicalRead<TLogical, TPhysical>.DirectReader? _directReader;
        private readonly LogicalRead<TLogical, TPhysical>.Converter _converter;
    }
}
