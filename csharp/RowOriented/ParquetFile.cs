using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using ParquetSharp.IO;
using ParquetSharp.Schema;

namespace ParquetSharp.RowOriented
{
    /// <summary>
    /// Static factory for creating row-oriented Parquet readers and writers.
    /// This is a higher-level API not part of apache-parquet-cpp.
    /// </summary>
    public static class ParquetFile
    {
        public static Action<Expression>? OnReadExpressionCreated;
        public static Action<Expression>? OnWriteExpressionCreated;

        /// <summary>
        /// Create a row-oriented reader from a file.
        /// </summary>
        public static ParquetRowReader<TTuple> CreateRowReader<TTuple>(string path)
        {
            var fields = GetFieldsAndProperties(typeof(TTuple));
            var readDelegate = GetOrCreateReadDelegate<TTuple>(fields);
            return new ParquetRowReader<TTuple>(path, readDelegate, fields);
        }

        public static ParquetRowReader<TTuple> CreateRowReader<TTuple>(string path, ReaderProperties readerProperties)
        {
            var fields = GetFieldsAndProperties(typeof(TTuple));
            var readDelegate = GetOrCreateReadDelegate<TTuple>(fields);
            return new ParquetRowReader<TTuple>(path, readerProperties, readDelegate, fields);
        }

        /// <summary>
        /// Create a row-oriented reader from an input stream.
        /// </summary>
        public static ParquetRowReader<TTuple> CreateRowReader<TTuple>(RandomAccessFile randomAccessFile)
        {
            var fields = GetFieldsAndProperties(typeof(TTuple));
            var readDelegate = GetOrCreateReadDelegate<TTuple>(fields);
            return new ParquetRowReader<TTuple>(randomAccessFile, readDelegate, fields);
        }

        public static ParquetRowReader<TTuple> CreateRowReader<TTuple>(RandomAccessFile randomAccessFile, ReaderProperties readerProperties)
        {
            var fields = GetFieldsAndProperties(typeof(TTuple));
            var readDelegate = GetOrCreateReadDelegate<TTuple>(fields);
            return new ParquetRowReader<TTuple>(randomAccessFile, readerProperties, readDelegate, fields);
        }

        /// <summary>
        /// Create a row-oriented writer to a file. By default, the column names are reflected from the tuple public fields and properties.
        /// </summary>
        public static ParquetRowWriter<TTuple> CreateRowWriter<TTuple>(
            string path,
            string[]? columnNames = null,
            Compression compression = Compression.Snappy,
            IReadOnlyDictionary<string, string>? keyValueMetadata = null)
        {
            var (schema, writeDelegate) = GetOrCreateWriteDelegate<TTuple>(columnNames);
            return new ParquetRowWriter<TTuple>(path, schema, compression, keyValueMetadata, writeDelegate);
        }

        public static ParquetRowWriter<TTuple> CreateRowWriter<TTuple>(
            string path,
            WriterProperties writerProperties,
            string[]? columnNames = null,
            IReadOnlyDictionary<string, string>? keyValueMetadata = null)
        {
            var (schema, writeDelegate) = GetOrCreateWriteDelegate<TTuple>(columnNames);
            return new ParquetRowWriter<TTuple>(path, schema, writerProperties, keyValueMetadata, writeDelegate);
        }

        /// <summary>
        /// Create a row-oriented writer to an output stream. By default, the column names are reflected from the tuple public fields and properties.
        /// </summary>
        public static ParquetRowWriter<TTuple> CreateRowWriter<TTuple>(
            OutputStream outputStream,
            string[]? columnNames = null,
            Compression compression = Compression.Snappy,
            IReadOnlyDictionary<string, string>? keyValueMetadata = null)
        {
            var (schema, writeDelegate) = GetOrCreateWriteDelegate<TTuple>(columnNames);
            return new ParquetRowWriter<TTuple>(outputStream, schema, compression, keyValueMetadata, writeDelegate);
        }

        public static ParquetRowWriter<TTuple> CreateRowWriter<TTuple>(
            OutputStream outputStream,
            WriterProperties writerProperties,
            string[]? columnNames = null,
            IReadOnlyDictionary<string, string>? keyValueMetadata = null)
        {
            var (schema, writeDelegate) = GetOrCreateWriteDelegate<TTuple>(columnNames);
            return new ParquetRowWriter<TTuple>(outputStream, schema, writerProperties, keyValueMetadata, writeDelegate);
        }

        private static ParquetRowReader<TTuple>.ReadAction GetOrCreateReadDelegate<TTuple>(MappedField[] fields)
        {
            return (ParquetRowReader<TTuple>.ReadAction) ReadDelegatesCache.GetOrAdd(typeof(TTuple), k => CreateReadDelegate<TTuple>(fields));
        }

        private static (GroupNode schema, ParquetRowWriter<TTuple>.WriteAction writeDelegate) GetOrCreateWriteDelegate<TTuple>(string[]? columnNames)
        {
            var (schema, writeDelegate) = WriteDelegates.GetOrAdd(typeof(TTuple), k => CreateWriteDelegate<TTuple>());
            if (columnNames != null)
            {
                // TODO: Fix this, maybe only allow column names for non-nested? Also, add a test to exercise this!
                //if (columnNames.Length != columns.Length)
                //{
                //    throw new ArgumentException("the length of column names does not mach the number of public fields and properties", nameof(columnNames));
                //}

                //columns = columns.Select((c, i) => new Column(c.LogicalSystemType, columnNames[i], c.LogicalTypeOverride, c.Length)).ToArray();
            }

            return (schema, (ParquetRowWriter<TTuple>.WriteAction) writeDelegate);
        }

        private static IEnumerable<(string VarSuffix, MappedField Field, bool IsLeaf)> DepthFirstFields(MappedField[] fields, string parentVarSuffix = "")
        {
            foreach (var field in fields)
            {
                var varSuffix = string.IsNullOrEmpty(parentVarSuffix) ? field.Name : $"{parentVarSuffix}_{field.Name}";
                foreach (var leaf in DepthFirstFields(field.GetChildren(), varSuffix))
                {
                    yield return leaf;
                }
                yield return (varSuffix, field, field.GetChildren().Length == 0);
            }
        }

        /// <summary>
        /// Returns a delegate to read rows from individual Parquet columns.
        /// </summary>
        private static ParquetRowReader<TTuple>.ReadAction CreateReadDelegate<TTuple>(MappedField[] fields)
        {
            // Parameters
            var reader = Expression.Parameter(typeof(ParquetRowReader<TTuple>), "reader");
            var tuples = Expression.Parameter(typeof(TTuple[]), "tuples");
            var length = Expression.Parameter(typeof(int), "length");

            var allFields = DepthFirstFields(fields).ToArray();

            // Use constructor or the property setters.
            var ctor = typeof(TTuple).GetConstructor(BindingFlags.Public | BindingFlags.Instance, null, fields.Select(f => f.Type).ToArray(), null);

            // Create a buffer for all mapped fields, including parents of nested fields.
            var buffers = allFields.ToDictionary(f => f.Field, f => Expression.Variable(f.Field.LogicalType.MakeArrayType(), $"buffer_{f.VarSuffix}"));
            var bufferAssigns = allFields.Select(f => (Expression) Expression.Assign(buffers[f.Field], Expression.NewArrayBounds(f.Field.LogicalType, length))).ToArray();

            // Read the columns from Parquet and populate the leaf buffers.
            var reads = allFields
                .Where(f => f.IsLeaf)
                .Select((f, i) => Expression.Call(
                    reader, GetReadMethod<TTuple>(f.Field.LogicalType), Expression.Constant(i), buffers[f.Field], length))
                .ToArray();

            // Loop over the tuples, constructing them from the column buffers and constructing any necessary intermediate nested objects.
            var index = Expression.Variable(typeof(int), "index");
            var constructRootObject = Expression.Assign(
                Expression.ArrayAccess(tuples, index),
                ctor == null
                    ? Expression.MemberInit(Expression.New(typeof(TTuple)),
                        fields.Select(f => Expression.Bind(f.Info, Expression.ArrayAccess(buffers[f], index))))
                    : Expression.New(ctor,
                        fields.Select(f => (Expression) Expression.ArrayAccess(buffers[f], index)))
            );
            var loopExpressions = new List<Expression>();
            foreach (var field in DepthFirstFields(fields))
            {
                if (!field.IsLeaf)
                {
                    var constructionExpression = NestedStructConstruction(field.Field, buffers, index);
                    loopExpressions.Add(Expression.Assign(
                        Expression.ArrayAccess(buffers[field.Field], index), constructionExpression));
                }
            }
            loopExpressions.Add(constructRootObject);
            var loop = For(index, Expression.Constant(0), Expression.NotEqual(index, length), Expression.PreIncrementAssign(index),
                Expression.Block(loopExpressions)
            );

            var body = Expression.Block(buffers.Values.ToArray(), bufferAssigns.Concat(reads).Concat(new[] {loop}));
            var lambda = Expression.Lambda<ParquetRowReader<TTuple>.ReadAction>(body, reader, tuples, length);
            OnReadExpressionCreated?.Invoke(lambda);
            return lambda.Compile();
        }

        /// <summary>
        /// Get an expression for constructing a struct mapped to a nested group
        /// </summary>
        private static Expression NestedStructConstruction(
            MappedField field, IReadOnlyDictionary<MappedField, ParameterExpression> buffers,
            ParameterExpression loopIndex)
        {
            var children = field.GetChildren();
            var fieldType = field.Type;
            if (IsNullable(fieldType, out var interiorType))
            {
                fieldType = interiorType;
            }

            var fieldCtor = fieldType.GetConstructor(BindingFlags.Public | BindingFlags.Instance, null, children.Select(f => f.Type).ToArray(), null);
            // Expression to construct a struct value, assuming it is non-null
            var constructionExpression =
                fieldCtor == null
                    ? (Expression) Expression.MemberInit(Expression.New(fieldType),
                        children.Select(f => Expression.Bind(f.Info,
                            GetNestedValueExpression(Expression.ArrayAccess(buffers[f], loopIndex), f))))
                    : Expression.New(fieldCtor,
                        children.Select(f => GetNestedValueExpression(Expression.ArrayAccess(buffers[f], loopIndex), f)));

            var childField = children.First();
            Expression childAccess = Expression.ArrayAccess(buffers[childField], loopIndex);

            // Handle when the field value is null
            if (IsNullable(field.Type, out _))
            {
                var fieldAccess = GetNestedValueExpression(childAccess, field);
                var valueCheck = Expression.MakeMemberAccess(fieldAccess, fieldAccess.Type.GetProperty("HasValue")!);
                var nullableConstructor = field.Type.GetConstructor(BindingFlags.Public | BindingFlags.Instance, null, new[] {interiorType}, null);
                var makeNonNull = Expression.New(nullableConstructor!, constructionExpression);
                var makeNull = Expression.Constant(null, field.Type);
                constructionExpression = Expression.Condition(valueCheck, makeNonNull, makeNull);
            }

            // Apply nesting for any parent types, also adding additional null checks required for nullable parents
            var parent = field.Parent;
            var parentType = field.Type;
            while (parent != null)
            {
                var nestedType = parentType;
                parentType = typeof(Nested<>).MakeGenericType(parentType);
                var nestedCtor = parentType.GetConstructor(BindingFlags.Public | BindingFlags.Instance, null, new[] {nestedType}, null);
                constructionExpression = Expression.New(nestedCtor!, constructionExpression);

                if (IsNullable(parent.Type, out _))
                {
                    var parentInteriorType = parentType;
                    parentType = typeof(Nullable<>).MakeGenericType(parentType);

                    var parentAccess = GetNestedValueExpression(childAccess, parent);
                    var valueCheck = Expression.MakeMemberAccess(parentAccess, parentAccess.Type.GetProperty("HasValue")!);
                    var nullableConstructor = parentType.GetConstructor(BindingFlags.Public | BindingFlags.Instance, null, new[] {parentInteriorType}, null);
                    var makeNonNull = Expression.New(nullableConstructor!, constructionExpression);
                    var makeNull = Expression.Constant(null, parentType);
                    constructionExpression =
                        Expression.Condition(valueCheck, makeNonNull, makeNull);
                }
                parent = parent.Parent;
            }

            return constructionExpression;
        }

        private static Expression NestValue(Expression expression, MappedField field)
        {
            var parentField = field.Parent;
            while (parentField != null)
            {
                var nestedType = typeof(Nested<>).MakeGenericType(expression.Type);
                var nestedCtor = nestedType.GetConstructor(BindingFlags.Public | BindingFlags.Instance, null, new[] {expression.Type}, null);
                expression = Expression.New(nestedCtor!, expression);
                if (IsNullable(parentField.Type, out _))
                {
                    var nullableType = typeof(Nullable<>).MakeGenericType(expression.Type);
                    var nullableCtor = nullableType.GetConstructor(BindingFlags.Public | BindingFlags.Instance, null, new[] {expression.Type}, null);
                    expression = Expression.New(nullableCtor!, expression);
                }
                parentField = parentField.Parent;
            }

            return expression;
        }

        /// <summary>
        /// Return an expression for getting the leaf value from a column with a potentially nested type
        /// </summary>
        /// <param name="valueExpression">Expression that provides an instance of the field's logical type</param>
        /// <param name="field">The mapped field for the column</param>
        private static Expression GetNestedValueExpression(Expression valueExpression, MappedField field)
        {
            var nestingDepth = 0;
            while (field.Parent != null)
            {
                ++nestingDepth;
                field = field.Parent;
            }

            var type = valueExpression.Type;
            for (int depth = 0; depth < nestingDepth; ++depth)
            {
                if (IsNullable(type, out var innerType))
                {
                    var nullableMember = type.GetProperty("Value");
                    valueExpression = Expression.MakeMemberAccess(valueExpression, nullableMember!);
                    type = innerType;
                }

                if (IsNested(type, out var nestedType))
                {
                    var nestedMember = type.GetField("Value");
                    valueExpression = Expression.MakeMemberAccess(valueExpression, nestedMember!);
                    type = nestedType;
                }
                else
                {
                    throw new ArgumentException("Expected a nested type");
                }
            }

            return valueExpression;
        }

        /// <summary>
        /// Return a delegate to write rows to individual Parquet columns, as well the column types and names.
        /// </summary>
        private static (GroupNode, ParquetRowWriter<TTuple>.WriteAction writeDelegate) CreateWriteDelegate<TTuple>()
        {
            var fields = GetFieldsAndProperties(typeof(TTuple));
            var schemaNode = BuildSchemaNode(fields);

            // Parameters
            var writer = Expression.Parameter(typeof(ParquetRowWriter<TTuple>), "writer");
            var tuples = Expression.Parameter(typeof(TTuple[]), "tuples");
            var length = Expression.Parameter(typeof(int), "length");

            var columnBodies = DepthFirstFields(fields).Where(f => f.IsLeaf).Select(f =>
            {
                // Column buffer
                var bufferType = f.Field.LogicalType.MakeArrayType();
                var buffer = Expression.Variable(bufferType, $"buffer_{f.VarSuffix}");
                var bufferAssign = Expression.Assign(buffer, Expression.NewArrayBounds(f.Field.LogicalType, length));
                var bufferReset = Expression.Assign(buffer, Expression.Constant(null, bufferType));

                // Loop over the tuples and fill the current column buffer.
                var index = Expression.Variable(typeof(int), "index");
                var loop = For(index, Expression.Constant(0), Expression.NotEqual(index, length), Expression.PreIncrementAssign(index),
                    Expression.Assign(
                        Expression.ArrayAccess(buffer, index),
                        GetNestedValue(Expression.ArrayAccess(tuples, index), f.Field)
                    )
                );

                // Write the buffer to Parquet.
                var writeCall = Expression.Call(writer, GetWriteMethod<TTuple>(buffer.Type.GetElementType()), buffer, length);

                return Expression.Block(
                    new[] {buffer, index},
                    bufferAssign,
                    loop,
                    writeCall,
                    bufferReset);
            });

            var body = Expression.Block(columnBodies);
            var lambda = Expression.Lambda<ParquetRowWriter<TTuple>.WriteAction>(body, writer, tuples, length);
            OnWriteExpressionCreated?.Invoke(lambda);
            return (schemaNode, lambda.Compile());
        }

        private static GroupNode BuildSchemaNode(MappedField[] fields)
        {
            var leafNodes = new List<Node>();
            foreach (var (_, field, isLeaf) in DepthFirstFields(fields))
            {
                if (isLeaf)
                {
                    leafNodes.Add(GetNode(field));
                }
                else
                {
                    var nullable = IsNullable(field.Type, out _);
                    var groupNode = GetGroupNode(field.SchemaName, nullable, leafNodes.ToArray());
                    leafNodes = new List<Node> {groupNode};
                }
            }

            return GetGroupNode("schema", false, leafNodes.ToArray());
        }

        /// <summary>
        /// Return an expression for getting the leaf-level logical type value to write
        /// </summary>
        private static Expression GetNestedValue(Expression rootObjectInstance, MappedField leafField, int checkedDepth = -1)
        {
            var parentStack = new List<MappedField>();
            MappedField? field = leafField;
            while (field.Parent != null)
            {
                parentStack.Add(field.Parent);
                field = field.Parent;
            }

            var leafExpression = rootObjectInstance;

            // First go down the hierarchy, getting values
            var depth = 0;
            while (parentStack.Count != 0)
            {
                var parent = parentStack.Last();
                parentStack.RemoveAt(parentStack.Count - 1);
                leafExpression = Expression.PropertyOrField(leafExpression, parent.Name);
                if (IsNullable(leafExpression.Type, out _))
                {
                    if (depth > checkedDepth)
                    {
                        // Add check for a value, and in the true case, recurse back in but set the
                        // checked depth so that we don't enter this branch again and assume we have a non-null value.
                        var nestedValue = GetNestedValue(rootObjectInstance, leafField, depth);
                        var nullValue = NestValue(Expression.Constant(null, UnwrapNesting(nestedValue.Type, depth)), parent);
                        return Expression.Condition(
                            Expression.PropertyOrField(leafExpression, "HasValue"),
                            nestedValue,
                            nullValue
                        );
                    }
                    else
                    {
                        // We know we have a valid value at this point, just get it
                        leafExpression = Expression.PropertyOrField(leafExpression, "Value");
                    }
                }
                ++depth;
            }
            leafExpression = Expression.PropertyOrField(leafExpression, leafField.Name);

            // Then go back up the hierarchy, nesting the value to return
            leafExpression = NestValue(leafExpression, leafField);

            return leafExpression;
        }

        private static MethodInfo GetReadMethod<TTuple>(Type type)
        {
            var genericMethod = typeof(ParquetRowReader<TTuple>).GetMethod(nameof(ParquetRowReader<TTuple>.ReadColumn), BindingFlags.NonPublic | BindingFlags.Instance);
            if (genericMethod == null)
            {
                throw new ArgumentException("could not find a ParquetReader generic read method");
            }

            return genericMethod.MakeGenericMethod(type);
        }

        private static MethodInfo GetWriteMethod<TTuple>(Type type)
        {
            var genericMethod = typeof(ParquetRowWriter<TTuple>).GetMethod(nameof(ParquetRowWriter<TTuple>.WriteColumn), BindingFlags.NonPublic | BindingFlags.Instance);
            if (genericMethod == null)
            {
                throw new ArgumentException("could not find a ParquetWriter generic writer method");
            }

            return genericMethod.MakeGenericMethod(type);
        }

        private static Expression For(
            ParameterExpression loopVar,
            Expression initValue, Expression condition, Expression increment,
            Expression loopContent)
        {
            var initAssign = Expression.Assign(loopVar, initValue);
            var breakLabel = Expression.Label("LoopBreak");

            return Expression.Block(new[] {loopVar},
                initAssign,
                Expression.Loop(
                    Expression.IfThenElse(
                        condition,
                        Expression.Block(
                            loopContent,
                            increment),
                        Expression.Break(breakLabel)),
                    breakLabel)
            );
        }

        private static MappedField[] GetFieldsAndProperties(Type type, MappedField? parent = null)
        {
            var list = new List<MappedField>();
            var flags = BindingFlags.Public | BindingFlags.Instance;

            if (IsNullable(type, out var interiorType))
            {
                type = interiorType;
            }

            if (type.IsGenericType && type.GetGenericTypeDefinition() == typeof(ValueTuple<,,,,,,,>))
            {
                throw new ArgumentException("System.ValueTuple TTuple types beyond 7 in length are not supported");
            }

            foreach (var field in type.GetFields(flags))
            {
                var mappedGroup = field.GetCustomAttribute<MapToGroupAttribute>()?.GroupName;
                var mappedColumn = field.GetCustomAttribute<MapToColumnAttribute>()?.ColumnName ?? mappedGroup;
                var mappedField = new MappedField(field, mappedColumn, field.FieldType, parent);
                var children = mappedGroup == null
                    ? Array.Empty<MappedField>()
                    : GetFieldsAndProperties(field.FieldType, mappedField);
                mappedField.Children = children;
                list.Add(mappedField);
            }

            foreach (var property in type.GetProperties(flags))
            {
                var mappedGroup = property.GetCustomAttribute<MapToGroupAttribute>()?.GroupName;
                var mappedColumn = property.GetCustomAttribute<MapToColumnAttribute>()?.ColumnName ?? mappedGroup;
                var mappedField = new MappedField(property, mappedColumn, property.PropertyType, parent);
                var children = mappedGroup == null
                    ? Array.Empty<MappedField>()
                    : GetFieldsAndProperties(property.PropertyType, mappedField);
                mappedField.Children = children;
                list.Add(mappedField);
            }

            // The order in which fields are processed is important given that when a tuple type is used in
            // ParquetFile.CreateRowWriter<TTuple>() with an array of column names it is expected that
            // the resulting parquet file correctly maps the name to the appropriate column type.
            //
            // However, neither Type.GetFields() nor Type.GetProperties() guarantee the order in which they return
            // fields or properties - importantly this means that they will not always be returned in
            // declaration order, not even for ValueTuples. The accepted means of returning fields and
            // properties in declaration order is to sort by MemberInfo.MetadataToken. This is done after
            // both the fields and properties have been gathered for greatest consistency.
            //
            // See https://stackoverflow.com/questions/8067493/if-getfields-doesnt-guarantee-order-how-does-layoutkind-sequential-work and
            // https://github.com/dotnet/corefx/issues/14606 for more detail.
            //
            // Note that most of the time GetFields() and GetProperties() _do_ return in declaration order and the times when they don't
            // are determined at runtime and not by the type. As a resut it is pretty much impossible to cover this with a unit test. Hence this
            // rather long comment aimed at avoiding accidental removal!
            return list.OrderBy(x => x.Info.MetadataToken).ToArray();
        }

        private static Column GetColumn(MappedField field)
        {
            var isDecimal = field.Type == typeof(decimal) || field.Type == typeof(decimal?);
            var decimalScale = field.Info.GetCustomAttributes(typeof(ParquetDecimalScaleAttribute))
                .Cast<ParquetDecimalScaleAttribute>()
                .SingleOrDefault();

            if (!isDecimal && decimalScale != null)
            {
                throw new ArgumentException($"field '{field.Name}' has a {nameof(ParquetDecimalScaleAttribute)} despite not being a decimal type");
            }

            if (isDecimal && decimalScale == null)
            {
                throw new ArgumentException($"field '{field.Name}' has no {nameof(ParquetDecimalScaleAttribute)} despite being a decimal type");
            }

            return new Column(field.Type, field.SchemaName, isDecimal ? LogicalType.Decimal(29, decimalScale!.Scale) : null);
        }

        private static Node GetNode(MappedField field)
        {
            return GetColumn(field).CreateSchemaNode();
        }

        private static GroupNode GetGroupNode(string groupName, bool nullable, Node[] children)
        {
            return new GroupNode(groupName, nullable ? Repetition.Optional : Repetition.Required, children);
        }

        private static bool IsNullable(Type type, out Type interiorType)
        {
            if (type.IsGenericType && type.GetGenericTypeDefinition() == typeof(Nullable<>))
            {
                interiorType = type.GetGenericArguments().Single();
                return true;
            }

            interiorType = typeof(object);
            return false;
        }

        private static bool IsNested(Type type, out Type nestedType)
        {
            if (type.IsGenericType && type.GetGenericTypeDefinition() == typeof(Nested<>))
            {
                nestedType = type.GetGenericArguments().Single();
                return true;
            }

            nestedType = typeof(object);
            return false;
        }

        private static Type UnwrapNesting(Type type, int depth)
        {
            for (var level = 0; level < depth; ++level)
            {
                if (IsNullable(type, out var interiorType))
                {
                    type = interiorType;
                }

                if (IsNested(type, out var nestedType))
                {
                    type = nestedType;
                }
                else
                {
                    throw new ArgumentException("Expected a nested type");
                }

            }
            return type;
        }

        private static readonly ConcurrentDictionary<Type, Delegate> ReadDelegatesCache =
            new ConcurrentDictionary<Type, Delegate>();

        private static readonly ConcurrentDictionary<Type, (GroupNode schema, Delegate writeDelegate)> WriteDelegates =
            new ConcurrentDictionary<Type, (GroupNode schema, Delegate writeDelegate)>();
    }
}
