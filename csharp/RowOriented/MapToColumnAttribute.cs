using System;

namespace ParquetSharp.RowOriented
{
    /// <summary>
    /// Explicitly map the given field to a specific column name.
    /// </summary>
    [AttributeUsage(AttributeTargets.Field | AttributeTargets.Property)]
    public sealed class MapToColumnAttribute : Attribute
    {
        public MapToColumnAttribute(string columnName)
        {
            ColumnName = columnName;
        }

        public readonly string ColumnName;
    }

    /// <summary>
    /// Explicitly map the given field to a specific schema group.
    /// </summary>
    [AttributeUsage(AttributeTargets.Field | AttributeTargets.Property)]
    public sealed class MapToGroupAttribute : Attribute
    {
        public MapToGroupAttribute(string groupName)
        {
            GroupName = groupName;
        }

        public readonly string GroupName;
    }
}
