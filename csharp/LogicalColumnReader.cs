using System;
using System.Collections;
using System.Collections.Generic;

namespace ParquetSharp
{
    /// <summary>
    /// Column reader transparently converting Parquet physical types to C# types.
    /// This is a higher-level API not part of apache-parquet-cpp.
    /// </summary>
    public abstract class LogicalColumnReader : LogicalColumnStream<ColumnReader>
    {
        protected LogicalColumnReader(ColumnReader columnReader, Type elementType, int bufferLength)
            : base(columnReader, columnReader.ColumnDescriptor, elementType, columnReader.ElementType, bufferLength)
        {
        }

        internal static LogicalColumnReader Create(ColumnReader columnReader, int bufferLength, Type? elementTypeOverride)
        {
            if (columnReader == null) throw new ArgumentNullException(nameof(columnReader));

            // If an elementTypeOverride is given, then we already know what the column reader logical system type should be.
            var columnLogicalTypeOverride = GetLeafElementType(elementTypeOverride);

            return columnReader.ColumnDescriptor.Apply(
                columnReader.LogicalTypeFactory,
                columnLogicalTypeOverride,
                new Creator(columnReader, bufferLength));
        }

        internal static LogicalColumnReader<TElement> Create<TElement>(ColumnReader columnReader, int bufferLength, Type? elementTypeOverride)
        {
            var reader = Create(columnReader, bufferLength, elementTypeOverride);

            try
            {
                return (LogicalColumnReader<TElement>) reader;
            }
            catch
            {
                reader.Dispose();
                throw;
            }
        }

        public bool HasNext => Source.HasNext;

        public abstract TReturn Apply<TReturn>(ILogicalColumnReaderVisitor<TReturn> visitor);

        private sealed class Creator : IColumnDescriptorVisitor<LogicalColumnReader>
        {
            public Creator(ColumnReader columnReader, int bufferLength)
            {
                _columnReader = columnReader;
                _bufferLength = bufferLength;
            }

            public LogicalColumnReader OnColumnDescriptor<TPhysical, TLogical, TElement>() where TPhysical : unmanaged
            {
                // PhysicalType is the actual type on disk (e.g. ByteArray).
                // LogicalType is the most nested logical type (e.g. string).
                // ElementType is the type represented by the column (e.g. string[][][]).

                // TODO !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
                // Improve this logic!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!

                if (typeof(TElement) == typeof(byte[]) || !IsCompoundType(typeof(TElement)))
                {
                    return new LeafLogicalColumnReader<TPhysical, TLogical, TElement>(_columnReader, _bufferLength);
                }
                return new CompoundLogicalColumnReader<TPhysical, TLogical, TElement>(_columnReader, _bufferLength);
            }

            private readonly ColumnReader _columnReader;
            private readonly int _bufferLength;
        }
    }

    public abstract class LogicalColumnReader<TElement> : LogicalColumnReader, IEnumerable<TElement>
    {
        protected LogicalColumnReader(ColumnReader columnReader, int bufferLength)
            : base(columnReader, typeof(TElement), bufferLength)
        {
        }

        public override TReturn Apply<TReturn>(ILogicalColumnReaderVisitor<TReturn> visitor)
        {
            return visitor.OnLogicalColumnReader(this);
        }

        public IEnumerator<TElement> GetEnumerator()
        {
            var buffer = new TElement[BufferLength];

            while (HasNext)
            {
                var read = ReadBatch(buffer);

                for (int i = 0; i != read; ++i)
                {
                    yield return buffer[i];
                }
            }
        }

        IEnumerator IEnumerable.GetEnumerator()
        {
            return GetEnumerator();
        }

        public TElement[] ReadAll(int rows)
        {
            var values = new TElement[rows];
            var read = ReadBatch(values);

            if (read != rows)
            {
                throw new ArgumentException($"read {read} rows, expected {rows} rows");
            }

            return values;
        }

        public int ReadBatch(TElement[] destination, int start, int length)
        {
            return ReadBatch(destination.AsSpan(start, length));
        }

        public abstract int ReadBatch(Span<TElement> destination);
    }
}
