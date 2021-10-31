namespace ParquetSharp
{
    public readonly struct Nested<T>
    {
        public readonly T Value;

        public Nested(T value)
        {
            Value = value;
        }
    }
}
