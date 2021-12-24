using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using ParquetSharp.IO;

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
            var (columns, writeDelegate) = GetOrCreateWriteDelegate<TTuple>(columnNames);
            return new ParquetRowWriter<TTuple>(path, columns, compression, keyValueMetadata, writeDelegate);
        }

        public static ParquetRowWriter<TTuple> CreateRowWriter<TTuple>(
            string path,
            WriterProperties writerProperties,
            string[]? columnNames = null,
            IReadOnlyDictionary<string, string>? keyValueMetadata = null)
        {
            var (columns, writeDelegate) = GetOrCreateWriteDelegate<TTuple>(columnNames);
            return new ParquetRowWriter<TTuple>(path, columns, writerProperties, keyValueMetadata, writeDelegate);
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
            var (columns, writeDelegate) = GetOrCreateWriteDelegate<TTuple>(columnNames);
            return new ParquetRowWriter<TTuple>(outputStream, columns, compression, keyValueMetadata, writeDelegate);
        }

        public static ParquetRowWriter<TTuple> CreateRowWriter<TTuple>(
            OutputStream outputStream,
            WriterProperties writerProperties,
            string[]? columnNames = null,
            IReadOnlyDictionary<string, string>? keyValueMetadata = null)
        {
            var (columns, writeDelegate) = GetOrCreateWriteDelegate<TTuple>(columnNames);
            return new ParquetRowWriter<TTuple>(outputStream, columns, writerProperties, keyValueMetadata, writeDelegate);
        }

        private static ParquetRowReader<TTuple>.ReadAction GetOrCreateReadDelegate<TTuple>(MappedField[] fields)
        {
            return (ParquetRowReader<TTuple>.ReadAction) ReadDelegatesCache.GetOrAdd(typeof(TTuple), k => CreateReadDelegate<TTuple>(fields));
        }

        private static (Column[] columns, ParquetRowWriter<TTuple>.WriteAction writeDelegate) GetOrCreateWriteDelegate<TTuple>(string[]? columnNames)
        {
            var (columns, writeDelegate) = WriteDelegates.GetOrAdd(typeof(TTuple), k => CreateWriteDelegate<TTuple>());
            if (columnNames != null)
            {
                if (columnNames.Length != columns.Length)
                {
                    throw new ArgumentException("the length of column names does not mach the number of public fields and properties", nameof(columnNames));
                }

                columns = columns.Select((c, i) => new Column(c.LogicalSystemType, columnNames[i], c.LogicalTypeOverride, c.Length)).ToArray();
            }

            return (columns, (ParquetRowWriter<TTuple>.WriteAction) writeDelegate);
        }

        private static IEnumerable<(string Name, MappedField Field, bool IsLeaf, int Level)> DepthFirstFields(MappedField[] fields, string path="", int level=0)
        {
            foreach (var field in fields)
            {
                var fieldPath = path + "_" + field.SchemaName;
                foreach (var leaf in DepthFirstFields(field.Children, fieldPath, level + 1))
                {
                    yield return leaf;
                }
                yield return (fieldPath, field, field.Children.Length == 0, level);
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
            var buffers = allFields.ToDictionary(f => f.Field, f => Expression.Variable(f.Field.GetLogicalType(f.Level).MakeArrayType(), $"buffer_{f.Name}"));
            var bufferAssigns = allFields.Select(f => (Expression) Expression.Assign(buffers[f.Field], Expression.NewArrayBounds(f.Field.GetLogicalType(f.Level), length))).ToArray();

            // Read the columns from Parquet and populate the leaf buffers.
            var reads = allFields.Where(f => f.IsLeaf).Select((f, i) => Expression.Call(reader, GetReadMethod<TTuple>(f.Field.GetLogicalType(f.Level)), Expression.Constant(i), buffers[f.Field], length)).ToArray();

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
                    // TODO: Handle nullable objects
                    var children = field.Field.Children;
                    var childLevel = field.Level + 1;
                    var fieldCtor = field.Field.Type.GetConstructor(BindingFlags.Public | BindingFlags.Instance, null, children.Select(f => f.Type).ToArray(), null);
                    var constructionExpression = Expression.Assign(
                        Expression.ArrayAccess(buffers[field.Field], index),
                        fieldCtor == null
                            ? Expression.MemberInit(Expression.New(field.Field.Type),
                                children.Select(f => Expression.Bind(f.Info, Expression.MakeMemberAccess(Expression.ArrayAccess(buffers[f], index), f.GetLogicalType(childLevel).GetField("Value")))))
                            : Expression.New(fieldCtor,
                                children.Select(f => (Expression) Expression.MakeMemberAccess(Expression.ArrayAccess(buffers[f], index), f.GetLogicalType(childLevel).GetField("Value"))))
                    );
                    loopExpressions.Add(constructionExpression);
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
        /// Return a delegate to write rows to individual Parquet columns, as well the column types and names.
        /// </summary>
        private static (Column[] columns, ParquetRowWriter<TTuple>.WriteAction writeDelegate) CreateWriteDelegate<TTuple>()
        {
            var fields = GetFieldsAndProperties(typeof(TTuple));
            var columns = fields.Select(GetColumn).ToArray();

            // Parameters
            var writer = Expression.Parameter(typeof(ParquetRowWriter<TTuple>), "writer");
            var tuples = Expression.Parameter(typeof(TTuple[]), "tuples");
            var length = Expression.Parameter(typeof(int), "length");

            var columnBodies = fields.Select(f =>
            {
                // Column buffer
                var bufferType = f.Type.MakeArrayType();
                var buffer = Expression.Variable(bufferType, $"buffer_{f.Name}");
                var bufferAssign = Expression.Assign(buffer, Expression.NewArrayBounds(f.Type, length));
                var bufferReset = Expression.Assign(buffer, Expression.Constant(null, bufferType));

                // Loop over the tuples and fill the current column buffer.
                var index = Expression.Variable(typeof(int), "index");
                var loop = For(index, Expression.Constant(0), Expression.NotEqual(index, length), Expression.PreIncrementAssign(index),
                    Expression.Assign(
                        Expression.ArrayAccess(buffer, index),
                        Expression.PropertyOrField(Expression.ArrayAccess(tuples, index), f.Name)
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
            return (columns, lambda.Compile());
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

        private static MappedField[] GetFieldsAndProperties(Type type)
        {
            var list = new List<MappedField>();
            var flags = BindingFlags.Public | BindingFlags.Instance;

            if (type.IsGenericType && type.GetGenericTypeDefinition() == typeof(ValueTuple<,,,,,,,>))
            {
                throw new ArgumentException("System.ValueTuple TTuple types beyond 7 in length are not supported");
            }

            foreach (var field in type.GetFields(flags))
            {
                var mappedGroup = field.GetCustomAttribute<MapToGroupAttribute>()?.GroupName;
                var children = mappedGroup == null
                    ? Array.Empty<MappedField>()
                    : GetFieldsAndProperties(field.FieldType);
                var mappedColumn = field.GetCustomAttribute<MapToColumnAttribute>()?.ColumnName ?? mappedGroup;
                list.Add(new MappedField(field, mappedColumn, field.FieldType, children));
            }

            foreach (var property in type.GetProperties(flags))
            {
                var mappedGroup = property.GetCustomAttribute<MapToGroupAttribute>()?.GroupName;
                var children = mappedGroup == null
                    ? Array.Empty<MappedField>()
                    : GetFieldsAndProperties(property.PropertyType);
                var mappedColumn = property.GetCustomAttribute<MapToColumnAttribute>()?.ColumnName ?? mappedGroup;
                list.Add(new MappedField(property, mappedColumn, property.PropertyType, children));
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

        private static readonly ConcurrentDictionary<Type, Delegate> ReadDelegatesCache =
            new ConcurrentDictionary<Type, Delegate>();

        private static readonly ConcurrentDictionary<Type, (Column[] columns, Delegate writeDelegate)> WriteDelegates =
            new ConcurrentDictionary<Type, (Column[] columns, Delegate writeDelegate)>();
    }
}
