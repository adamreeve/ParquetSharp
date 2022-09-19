using System;
using ParquetSharp.IO;
using NUnit.Framework;

namespace ParquetSharp.Test
{
    [TestFixture]
    internal static class TestLogicalColumnWriter
    {
        [Test]
        public static void TestInvalidElementTypeErrorMessage()
        {
            var schemaColumns = new Column[] {new Column<int?>("col")};

            using var buffer = new ResizableBuffer();

            using var outStream = new BufferOutputStream(buffer);
            using var writer = new ParquetFileWriter(outStream, schemaColumns);
            using var rowGroupWriter = writer.AppendRowGroup();
            using var colWriter = rowGroupWriter.NextColumn();

            var exception = Assert.Throws<InvalidCastException>(() => colWriter.LogicalWriter<int>())!;

            Assert.That(exception.Message, Is.EqualTo(
                "Specified element type 'System.Int32' for column 'col' " +
                "is not compatible with the expected logical type 'System.Nullable`1[System.Int32]'"));

            writer.Close();
        }
    }
}
