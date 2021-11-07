using System;
using System.Collections.Generic;
using System.Linq;

namespace ParquetSharp
{
    public abstract class LogicalColumnStream<TSource> : IDisposable
        where TSource : class, IDisposable
    {
        protected LogicalColumnStream(TSource source, ColumnDescriptor descriptor, Type elementType, Type physicalType, int bufferLength)
        {
            Source = source ?? throw new ArgumentNullException(nameof(source));
            ColumnDescriptor = descriptor ?? throw new ArgumentNullException(nameof(descriptor));
            BufferLength = bufferLength;
            LogicalType = descriptor.LogicalType;

            Buffer = Array.CreateInstance(physicalType, bufferLength);
            DefLevels = descriptor.MaxDefinitionLevel == 0 ? null : new short[bufferLength];
            RepLevels = descriptor.MaxRepetitionLevel == 0 ? null : new short[bufferLength];
        }

        public virtual void Dispose()
        {
            Source.Dispose();
        }

        protected static bool RepLevelsRequired(Type type)
        {
            if (type.IsArray)
            {
                return true;
            }
            if (IsNullable(type, out var innerTypeNullable))
            {
                return RepLevelsRequired(innerTypeNullable);
            }
            if (IsNested(type, out var innerTypeNested))
            {
                return RepLevelsRequired(innerTypeNested);
            }
            return false;
        }

        protected static Type? GetLeafElementType(Type? type)
        {
            while (type != null && type != typeof(byte[]) && type.IsArray)
            {
                type = type.GetElementType();
            }

            return type;
        }

        protected static bool IsNullable(Type type, out Type inner)
        {
            if (!type.IsGenericType || type.GetGenericTypeDefinition() != typeof(Nullable<>))
            {
                inner = null!;
                return false;
            }
            inner = type.GetGenericArguments().Single();
            return true;
        }

        protected static bool IsNested(Type type, out Type inner)
        {
            if (!type.IsGenericType || type.GetGenericTypeDefinition() != typeof(Nested<>))
            {
                inner = null!;
                return false;
            }
            inner = type.GetGenericArguments().Single();
            return true;
        }

        protected static List<Schema.Node> GetSchemaNode(Schema.Node node)
        {
            var schemaNodes = new List<Schema.Node>();
            for (var n = node; n != null; n = n.Parent)
            {
                schemaNodes.Add(n);
            }
            schemaNodes.RemoveAt(schemaNodes.Count - 1); // we don't need the schema root
            schemaNodes.Reverse(); // root to leaf
            return schemaNodes;
        }

        protected static bool IsCompoundType(Type elementType)
        {
            elementType = NonNullable(elementType);
            return IsNested(elementType, out _) || elementType.IsArray;
        }

        private static Type NonNullable(Type type) =>
            type.IsGenericType && type.GetGenericTypeDefinition() == typeof(Nullable<>) ? type.GetGenericArguments().Single() : type;

        public TSource Source { get; }
        public ColumnDescriptor ColumnDescriptor { get; }
        public int BufferLength { get; }
        public LogicalType LogicalType { get; }

        protected readonly Array Buffer;
        protected readonly short[]? DefLevels;
        protected readonly short[]? RepLevels;
    }
}
