using System;
using System.Reflection;

namespace ParquetSharp.RowOriented
{
    internal readonly struct MappedField
    {
        public readonly MemberInfo Info;
        public readonly string? MappedSchemaName;
        public readonly Type Type;
        public readonly MappedField[] Children;

        public MappedField(MemberInfo memberInfo, string? schemaName, Type type, MappedField[] children)
        {
            Info = memberInfo;
            MappedSchemaName = schemaName;
            Type = type;
            Children = children;
        }

        public string Name => Info.Name;

        public string SchemaName => MappedSchemaName ?? Info.Name;

        /// <summary>
        /// Apply any nesting to get the final logical type of the leaf column
        /// </summary>
        /// <returns></returns>
        public Type GetLogicalType(int nestingLevel)
        {
            var type = Type;
            for (var i = 1; i < nestingLevel; ++i)
            {
                type = typeof(Nested<>).MakeGenericType(type);
            }

            return type;
        }
    }
}