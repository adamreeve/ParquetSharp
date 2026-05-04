using ParquetSharp;

namespace SegfaultRepro;

public static class Program
{
    public static void Main()
    {
        for (var i = 0; i < 10; ++i)
        {
            Console.WriteLine($"Run {i}");
            Thread thread = new Thread(RunThread);
            thread.Start();
            thread.Join();
        }
    }

    private static void RunThread()
    {
        var filename = "segfault-test.parquet";

        int[] values = Enumerable.Range(0, 1_000_000).ToArray();

        using (var fileWriter = new ParquetFileWriter(filename, new Column[] { new Column<int>("Value") }))
        {
            using var rowGroupWriter = fileWriter.AppendRowGroup();
            using var valueWriter = rowGroupWriter.NextColumn().LogicalWriter<int>();
            valueWriter.WriteBatch(values);
            fileWriter.Close();
        }
    }
}
