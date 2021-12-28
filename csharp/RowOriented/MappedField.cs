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
        public readonly MappedField[] Children;

        public MappedField(MemberInfo memberInfo, string? schemaName, Type type, MappedField[] children, bool[] parentNullability)
        {
            Info = memberInfo;
            MappedSchemaName = schemaName;
            Type = type;
            Children = children;
            LogicalType = GetLogicalType(type, parentNullability);
        }

        public string Name => Info.Name;

        public string SchemaName => MappedSchemaName ?? Info.Name;

        /// <summary>
        /// Get array of members that must be accessed to reach the non-nested value
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
        /// <returns></returns>
        private static Type GetLogicalType(Type type, bool[] parentNullability)
        {
            foreach (var nullable in parentNullability)
            {
                type = typeof(Nested<>).MakeGenericType(type);
                if (nullable)
                {
                    type = typeof(Nullable<>).MakeGenericType(type);
                }
            }

            return type;
        }
    }
}
