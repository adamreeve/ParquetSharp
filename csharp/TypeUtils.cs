using System;
using System.Linq;

namespace ParquetSharp
{
    internal static class TypeUtils
    {
        public static bool IsNullable(Type type, out Type inner)
        {
            if (!type.IsGenericType || type.GetGenericTypeDefinition() != typeof(Nullable<>))
            {
                inner = null!;
                return false;
            }
            inner = type.GetGenericArguments().Single();
            return true;
        }

        public static bool IsNested(Type type, out Type inner)
        {
            if (!type.IsGenericType || type.GetGenericTypeDefinition() != typeof(Nested<>))
            {
                inner = null!;
                return false;
            }
            inner = type.GetGenericArguments().Single();
            return true;
        }

        public static bool IsNullableNested(Type type, out Type inner)
        {
            if (IsNullable(type, out var nullableInner) && IsNested(nullableInner, out var nestedInner))
            {
                inner = nestedInner;
                return true;
            }
            inner = null!;
            return false;
        }

        public static bool IsReadOnlyMemory(Type type, out Type inner)
        {
            if (type.IsGenericType && type.GetGenericTypeDefinition() == typeof(ReadOnlyMemory<>))
            {
                inner = type.GetGenericArguments().Single();
                return true;
            }
            inner = null!;
            return false;
        }

        /// <summary>
        /// Whether type type is an array, or an equivalent type that can be used for writing array values
        /// </summary>
        public static bool IsArrayLike(Type type, out Type inner)
        {
            if (type.IsArray)
            {
                inner = type.GetElementType()!;
                return true;
            }
            if (IsReadOnlyMemory(type, out var romType))
            {
                inner = romType;
                return true;
            }
            if (IsNullable(type, out var nullableInner) && IsReadOnlyMemory(nullableInner, out var nullableRomType))
            {
                inner = nullableRomType;
                return true;
            }
            inner = null!;
            return false;
        }

        public static Type? GetLeafElementType(Type? type)
        {
            while (type != null)
            {
                if (type != typeof(byte[]) && IsArrayLike(type, out var elementType))
                {
                    type = elementType;
                }
                else if (IsNested(type, out var nestedType))
                {
                    type = nestedType;
                }
                else if (IsNullableNested(type, out var nullableNestedType))
                {
                    type = nullableNestedType;
                }
                else
                {
                    break;
                }
            }

            return type;
        }
    }
}
