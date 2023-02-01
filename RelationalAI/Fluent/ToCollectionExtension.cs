using System;
using System.Collections;
using System.Collections.Generic;
using System.Data;
using System.Globalization;
using System.Linq;
using System.Text;
using Microsoft.Data.Analysis;

namespace RelationalAI.Fluent
{
    public static class ToCollectionExtension
    {
        /// <summary>
        /// Transforms enumerable <c>ArrowRelation</c> to <c>ArrayList</c>. Use with Results from REL server.
        /// <example>
        /// <code>tx.Results.ToList()</code>
        /// </example>
        /// </summary>
        /// <param name="arrowRelations"> from REL server</param>
        /// <param name="col"> which column to return, defaults to -1 (last column)</param>
        /// <returns>ArrayList</returns>
        public static ArrayList ToArrayList(this IEnumerable<ArrowRelation> arrowRelations, int col = -1)
        {
            var outputRelations = arrowRelations.Where(r => r.RelationId.StartsWith("/:output/"));
            var columnSpec = Rel.DeriveColumnSpec(outputRelations.Select(r => r.RelationId));
            if (col == -1)
            {
                col = columnSpec.Length - 1;
            }

            if (columnSpec.Length <= col)
            {
                return new ArrayList();
            }

            var values = new ArrayList();


            foreach (var arrowRelation in outputRelations)
            {
                var dataFrame = DataFrame.FromArrowRecordBatch(arrowRelation.Table);
                if (!dataFrame.Rows.Any()) // Special case where leading columns are only symbols
                {
                    var value = ReadValueFrom(arrowRelation, col);
                    values.Add(value);
                }
                else
                {
                    foreach (var r in dataFrame.Rows)
                    {
                        var value = ReadValueFrom(arrowRelation, col, r);
                        values.Add(value);
                    }
                }
            }

            return values;
        }

        /// <summary>
        /// Transforms enumerable <c>ArrowRelation</c> to <c>HashSet</c>. Use with Results from REL server.
        /// <example>
        /// <code>tx.Results.ToSet()</code>
        /// </example>
        /// </summary>
        /// <param name="arrowRelations"> from REL server</param>
        /// <param name="col"> which column to return, defaults to -1 (last column)</param>
        /// <typeparam name="T">primitive type</typeparam>
        /// <returns>HashSet</returns>
        public static HashSet<T> ToSet<T>(this IEnumerable<ArrowRelation> arrowRelations, int col = -1)
        {
            var outputRelations = arrowRelations.Where(r => r.RelationId.StartsWith("/:output/"));
            var columnSpec = Rel.DeriveColumnSpec(outputRelations.Select(r => r.RelationId));
            if (col == -1)
            {
                col = columnSpec.Length - 1;
            }

            if (columnSpec.Length <= col)
            {
                return new HashSet<T>();
            }

            var values = new HashSet<T>();
            foreach (var arrowRelation in outputRelations)
            {
                var dataFrame = DataFrame.FromArrowRecordBatch(arrowRelation.Table);
                if (!dataFrame.Rows.Any()) // Special case where leading columns are only symbols
                {
                    var value = ReadValueFrom(arrowRelation, col);
                    values.Add((T)Convert.ChangeType(value, typeof(T), CultureInfo.InvariantCulture));
                }
                else
                {
                    foreach (var r in dataFrame.Rows)
                    {
                        var value = ReadValueFrom(arrowRelation, col, r);
                        values.Add((T)Convert.ChangeType(value, typeof(T), CultureInfo.InvariantCulture));
                    }
                }
            }

            return values;
        }

        /// <summary>
        /// Transforms enumerable <c>ArrowRelation</c> to <c>HashSet</c>. Use with Results from REL server.
        /// <example>
        /// <code>tx.Results.ToSet()</code>
        /// </example>
        /// </summary>
        /// <param name="arrowRelations"> from REL server</param>
        /// <param name="col"> which column to return, defaults to -1 (last column)</param>
        /// <typeparam name="T">primitive type</typeparam>
        /// <returns>List</returns>
        public static List<T> ToList<T>(this IEnumerable<ArrowRelation> arrowRelations, int col = -1)
        {
            var outputRelations = arrowRelations.Where(r => r.RelationId.StartsWith("/:output/"));
            var columnSpec = Rel.DeriveColumnSpec(outputRelations.Select(r => r.RelationId));
            if (col == -1)
            {
                col = columnSpec.Length - 1;
            }

            if (columnSpec.Length <= col)
            {
                return new List<T>();
            }

            var values = new List<T>();
            foreach (var arrowRelation in outputRelations)
            {
                var dataFrame = DataFrame.FromArrowRecordBatch(arrowRelation.Table);
                if (!dataFrame.Rows.Any()) // Special case where leading columns are only symbols
                {
                    var value = ReadValueFrom(arrowRelation, col);
                    values.Add((T)Convert.ChangeType(value, typeof(T), CultureInfo.InvariantCulture));
                }
                else
                {
                    foreach (var r in dataFrame.Rows)
                    {
                        var value = ReadValueFrom(arrowRelation, col, r);
                        values.Add((T)Convert.ChangeType(value, typeof(T), CultureInfo.InvariantCulture));
                    }
                }
            }

            return values;
        }

        private static object ReadValueFrom(
            ArrowRelation arrowRelation,
            int col,
            IEnumerable<object> values = null)
        {
            values ??= Array.Empty<string>();
            var columnList = Rel.ColumnListFrom(arrowRelation.RelationId);
            var columnCount = columnList.Count();

            if (columnCount <= col)
            {
                return null;
            }

            var dataRow = new object[columnCount];
            using (var valueEnumerator = values.GetEnumerator())
            {
                valueEnumerator.MoveNext(); // Move to first
                foreach (var (field, i) in columnList.WithIndex())
                {
                    if (Rel.IsSymbol(field))
                    {
                        dataRow[i] = field;
                    }
                    else
                    {
                        dataRow[i] = valueEnumerator.Current;
                        valueEnumerator.MoveNext();
                    }
                }
            }

            return dataRow[col];
        }

        public static ArrayList Print(this ArrayList list, Action<string> writeln)
        {
            var sb = new StringBuilder();
            sb.Append('[');
            sb.Append(string.Join(", ", list.ToArray()));
            sb.Append(']');
            writeln(sb.ToString());
            return list;
        }

        public static IEnumerable<T> Print<T>(this IEnumerable<T> enumerable, Action<string> writeln)
        {
            var sb = new StringBuilder();
            sb.Append('[');
            sb.Append(string.Join(", ", enumerable));
            sb.Append(']');
            writeln(sb.ToString());
            return enumerable;
        }
    }
}