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
        /// Get an array of members that must be accessed from an instance of the logical type to reach the non-nested value
        /// </summary>
        public MemberInfo[] GetValueMemberChain()
        {
            var members = new List<MemberInfo>();
            var type = LogicalType;
            while (true)
            {
                var levelMembers = new List<MemberInfo>();
                if (type.IsGenericType && type.GetGenericTypeDefinition() == typeof(Nullable<>))
                {
                    levelMembers.Add(type.GetProperty("Value"));
                    type = type.GenericTypeArguments.Single();
                }

                if (type.IsGenericType && type.GetGenericTypeDefinition() == typeof(Nested<>))
                {
                    levelMembers.Add(type.GetField("Value"));
                    type = type.GenericTypeArguments.Single();
                }
                else
                {
                    break;
                }

                members.AddRange(levelMembers);
            }
            return members.ToArray();
        }

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
