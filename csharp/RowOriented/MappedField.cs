using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;

namespace ParquetSharp.RowOriented
{
    internal class MappedField
    {
        public readonly MemberInfo Info;
        public readonly string? MappedSchemaName;
        public readonly Type Type;
        public readonly Type LogicalType;
        public readonly MappedField? Parent;
        public MappedField[]? Children;

        public MappedField(MemberInfo memberInfo, string? schemaName, Type type, MappedField? parent)
        {
            Info = memberInfo;
            MappedSchemaName = schemaName;
            Type = type;
            Parent = parent;
            LogicalType = GetLogicalType(type, parent);
        }

        public string Name => Info.Name;

        public string SchemaName => MappedSchemaName ?? Info.Name;

        public MappedField[] GetChildren() => Children ?? Array.Empty<MappedField>();

        /// <summary>
        /// Apply any nesting to get the final logical type of the leaf column
        /// </summary>
        private static Type GetLogicalType(Type type, MappedField? parent)
        {
            while (parent != null)
            {
                type = typeof(Nested<>).MakeGenericType(type);
                if (parent.Type.IsGenericType && parent.Type.GetGenericTypeDefinition() == typeof(Nullable<>))
                {
                    type = typeof(Nullable<>).MakeGenericType(type);
                }
                parent = parent.Parent;
            }

            return type;
        }
    }
}
