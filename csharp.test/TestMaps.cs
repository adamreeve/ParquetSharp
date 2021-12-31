using System;
using System.IO;
using NUnit.Framework;
using ParquetSharp.IO;
using ParquetSharp.Schema;

namespace ParquetSharp.Test
{
    [TestFixture]
    public class TestMaps
    {
        /// <summary>
        /// See generate_parquet.py
        /// </summary>
        [Test]
        public void CanReadMaps()
        {
            var directory = Path.GetDirectoryName(System.Reflection.Assembly.GetExecutingAssembly().Location);
            var path = Path.Combine(directory!, "TestFiles/map.parquet");

            using var fileReader = new ParquetFileReader(path);
            var rowGroupReader = fileReader.RowGroup(0);

            using var col0Reader = rowGroupReader.Column(0).LogicalReader<string[]>();
            using var col1Reader = rowGroupReader.Column(1).LogicalReader<string[]>();
            using var col2Reader = rowGroupReader.Column(2).LogicalReader<string>();

            var col0Actual = col0Reader.ReadAll(2);
            var col1Actual = col1Reader.ReadAll(2);
            var col2Actual = col2Reader.ReadAll(2);
        }

        [Test]
        public static void CanRoundtripOptionalMaps()
        {
            var keys = new[] {new[] {"k1", "k2"}, new[] {"k3", "k4"}, null, new string[0]};
            var values = new[] {new[] {"v1", "v2"}, new[] {"v3", "v4"}, null, new string[0]};

            DoRoundtripTest(true, keys!, values!);
        }

        [Test]
        public static void CanRoundtripRequiredMaps()
        {
            var keys = new[] {new[] {"k1", "k2"}, new[] {"k3", "k4"}, new string[0]};
            var values = new[] {new[] {"v1", "v2"}, new[] {"v3", "v4"}, new string[0]};

            DoRoundtripTest(false, keys, values);
        }

        [Test]
        public static void NullsInRequiredMapGiveException()
        {
            var pool = MemoryPool.GetDefaultMemoryPool();
            Assert.AreEqual(0, pool.BytesAllocated);

            using (var buffer = new ResizableBuffer())
            {
                using var outStream = new BufferOutputStream(buffer);
                var writerProperties = new WriterPropertiesBuilder().Build();
                var schemaNode = CreateMapSchema(false);
                using var fileWriter = new ParquetFileWriter(outStream, schemaNode, writerProperties);
                using var rowGroupWriter = fileWriter.AppendRowGroup();
                using var colWriterKeys = rowGroupWriter.NextColumn().LogicalWriter<string[]>();

                var keys = new[] { new[] { "k1", "k2" }, new[] { "k3", "k4" }, null, new string[0] };

                // Writing a column containing a null should throw an exception because the schema says values are required
                var exception = Assert.Throws<InvalidOperationException>(() => colWriterKeys.WriteBatch(keys!))!;
                Assert.AreEqual("Cannot write a null array value for a required array column", exception.Message);

                // We will also get an exception because we haven't written any data
                var closeException = Assert.Throws<ParquetException>(() => fileWriter.Close())!;
                Assert.IsTrue(closeException.Message.Contains("Only 0 out of 2 columns are initialized"));
            }

            Assert.AreEqual(0, pool.BytesAllocated);
        }

        private static void DoRoundtripTest(bool optional, string[][] keys, string[][] values)
        {
            var pool = MemoryPool.GetDefaultMemoryPool();
            Assert.AreEqual(0, pool.BytesAllocated);

            using (var buffer = new ResizableBuffer())
            {
                using (var outStream = new BufferOutputStream(buffer))
                {
                    var writerProperties = new WriterPropertiesBuilder().Build();
                    var schemaNode = CreateMapSchema(optional);
                    using var fileWriter = new ParquetFileWriter(outStream, schemaNode, writerProperties);
                    using var rowGroupWriter = fileWriter.AppendRowGroup();

                    using var colWriterKeys = rowGroupWriter.NextColumn().LogicalWriter<string[]>();
                    colWriterKeys.WriteBatch(keys);

                    using var colWriterValues = rowGroupWriter.NextColumn().LogicalWriter<string[]>();
                    colWriterValues.WriteBatch(values);

                    fileWriter.Close();
                }

                // Read it back.
                using var inStream = new BufferReader(buffer);
                using var fileReader = new ParquetFileReader(inStream);
                using var rowGroup = fileReader.RowGroup(0);

                using var keysColReader = rowGroup.Column(0).LogicalReader<string[]>();
                var keysActual = keysColReader.ReadAll(values.Length);
                using var valuesColReader = rowGroup.Column(1).LogicalReader<string[]>();
                var valuesActual = valuesColReader.ReadAll(values.Length);

                Assert.AreEqual(keys, keysActual);
                Assert.AreEqual(values, valuesActual);
            }

            Assert.AreEqual(0, pool.BytesAllocated);
        }

        private static GroupNode CreateMapSchema(bool optional)
        {
            return new GroupNode(
                "schema",
                Repetition.Required,
                new Node[]
                {
                    new GroupNode(
                        "col1",
                        optional ? Repetition.Optional : Repetition.Required,
                        new Node[]
                        {
                            new GroupNode(
                                "key_value",
                                Repetition.Repeated,
                                new Node[]
                                {
                                    new PrimitiveNode("key", Repetition.Required, LogicalType.String(), PhysicalType.ByteArray),
                                    new PrimitiveNode("value", Repetition.Optional, LogicalType.String(), PhysicalType.ByteArray)
                                }
                            )
                        },
                        LogicalType.Map()
                    )
                }
            );
        }
    }
}
