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

            var col0Actual = rowGroupReader.Column(0).LogicalReader<string[]>().ReadAll(2);
            var col1Actual = rowGroupReader.Column(1).LogicalReader<string[]>().ReadAll(2);
            var col2Actual = rowGroupReader.Column(2).LogicalReader<string>().ReadAll(2);
        }

        [Test]
        public static void CanRoundtripMaps()
        {
            var schemaNode = CreateMapSchema();

            using var buffer = new ResizableBuffer();

            var keysExpected = new[] {new[] {"a", "b"}, new[] {"c", "d"}};
            var valuesExpected = new[] {new[] {"e", "f"}, new[] {"g", "h"}};

            using (var outStream = new BufferOutputStream(buffer))
            {
                var writerProperties = new WriterPropertiesBuilder().Build();
                using var fileWriter = new ParquetFileWriter(outStream, schemaNode, writerProperties);
                using var rowGroupWriter = fileWriter.AppendRowGroup();

                using var colWriterKeys = rowGroupWriter.NextColumn().LogicalWriter<string[]>();
                colWriterKeys.WriteBatch(keysExpected);

                using var colWriterValues = rowGroupWriter.NextColumn().LogicalWriter<string[]>();
                colWriterValues.WriteBatch(valuesExpected);

                fileWriter.Close();
            }

            // Read it back.
            using var inStream = new BufferReader(buffer);
            using var fileReader = new ParquetFileReader(inStream);
            using var rowGroup = fileReader.RowGroup(0);

            var keysActual = rowGroup.Column(0).LogicalReader<string[]>().ReadAll(2);
            var valuesActual = rowGroup.Column(1).LogicalReader<string[]>().ReadAll(2);

            Assert.AreEqual(keysExpected, keysActual);
            Assert.AreEqual(valuesExpected, valuesActual);
        }

        private static GroupNode CreateMapSchema()
        {
            return new GroupNode(
                "schema",
                Repetition.Required,
                new Node[]
                {
                    new GroupNode(
                        "col1",
                        Repetition.Optional,
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
