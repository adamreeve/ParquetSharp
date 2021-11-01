using System;
using ParquetSharp.Schema;

namespace ParquetSharp
{
    /// <summary>
    /// Column writer transparently converting C# types to Parquet physical types.
    /// This is a higher-level API not part of apache-parquet-cpp.
    /// </summary>
    public abstract class LogicalColumnWriter : LogicalColumnStream<ColumnWriter>
    {
        protected LogicalColumnWriter(ColumnWriter columnWriter, Type elementType, int bufferLength)
            : base(columnWriter, columnWriter.ColumnDescriptor, elementType, columnWriter.ElementType, bufferLength)
        {
        }

        internal static LogicalColumnWriter Create(ColumnWriter columnWriter, int bufferLength, Type? elementTypeOverride)
        {
            if (columnWriter == null) throw new ArgumentNullException(nameof(columnWriter));

            // If the file writer was constructed with a Columns[] argument, or if an elementTypeOverride is given,
            // then we already know what the column writer logical system type should be.
            var columns = columnWriter.RowGroupWriter.ParquetFileWriter.Columns;
            var columnLogicalTypeOverride = GetLeafElementType(elementTypeOverride ?? columns?[columnWriter.ColumnIndex].LogicalSystemType);

            return columnWriter.ColumnDescriptor.Apply(
                columnWriter.LogicalTypeFactory,
                columnLogicalTypeOverride,
                new Creator(columnWriter, bufferLength));
        }

        internal static LogicalColumnWriter<TElementType> Create<TElementType>(ColumnWriter columnWriter, int bufferLength, Type? elementTypeOverride)
        {
            var writer = Create(columnWriter, bufferLength, elementTypeOverride);

            try
            {
                return (LogicalColumnWriter<TElementType>) writer;
            }
            catch
            {
                writer.Dispose();
                throw;
            }
        }

        public abstract TReturn Apply<TReturn>(ILogicalColumnWriterVisitor<TReturn> visitor);

        private sealed class Creator : IColumnDescriptorVisitor<LogicalColumnWriter>
        {
            public Creator(ColumnWriter columnWriter, int bufferLength)
            {
                _columnWriter = columnWriter;
                _bufferLength = bufferLength;
            }

            public LogicalColumnWriter OnColumnDescriptor<TPhysical, TLogical, TElement>() where TPhysical : unmanaged
            {
                // PhysicalType is the actual type on disk (e.g. ByteArray).
                // LogicalType is the most nested logical type (e.g. string).
                // ElementType is the type represented by the column (e.g. string[][][]).
                if (!typeof(TElement).IsArray || typeof(TElement) == typeof(byte[]))
                {
                    return new LeafLogicalColumnWriter<TPhysical, TLogical, TElement>(_columnWriter, _bufferLength);
                }
                return new ArrayLogicalColumnWriter<TPhysical, TLogical, TElement>(_columnWriter, _bufferLength);
            }

            private readonly ColumnWriter _columnWriter;
            private readonly int _bufferLength;
        }
    }

    public abstract class LogicalColumnWriter<TElement> : LogicalColumnWriter
    {
        protected LogicalColumnWriter(ColumnWriter columnWriter, int bufferLength)
            : base(columnWriter, typeof(TElement), bufferLength)
        {
        }

        public override TReturn Apply<TReturn>(ILogicalColumnWriterVisitor<TReturn> visitor)
        {
            return visitor.OnLogicalColumnWriter(this);
        }

        public void WriteBatch(TElement[] values)
        {
            WriteBatch(values.AsSpan());
        }

        public void WriteBatch(TElement[] values, int start, int length)
        {
            WriteBatch(values.AsSpan(start, length));
        }

        public abstract void WriteBatch(ReadOnlySpan<TElement> values);
    }
}
