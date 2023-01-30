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
            var columnSpec = DeriveColumnSpec(outputRelations.Select(r => r.RelationId));
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
            var columnSpec = DeriveColumnSpec(outputRelations.Select(r => r.RelationId));
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
            var columnSpec = DeriveColumnSpec(outputRelations.Select(r => r.RelationId));
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
            var columnList = ColumnListFrom(arrowRelation.RelationId);
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
                    if (IsSymbol(field))
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


        /// <summary>
        /// Creates an enumerable from REL server named RecordBatch that strips "/:output/" and splits into parts.
        /// <example>
        /// Example:
        ///  <code>"/:output/String/:height/Int64/:mm/:metric"</code>
        /// Returns:
        ///  <code>"{"String", ":height", "Int64", ":mm", ":metric"}</code>
        /// </example>
        /// </summary>
        /// <param name="arrowName">RecordBatch name</param>
        /// <returns>Type parts</returns>
        private static IEnumerable<string> ColumnListFrom(string arrowName)
        {
            return String.IsNullOrWhiteSpace(arrowName) ? Array.Empty<string>() : arrowName[9..].Split('/');
        }

        /// <summary>
        /// Determines if supplied string is a REL symbol.
        /// </summary>
        /// <param name="name">Field value or name</param>
        /// <returns>True if REL symbol</returns>
        private static bool IsSymbol(string name)
        {
            return name.StartsWith(':');
        }

        /// <summary>
        /// Using an enumeration of column names output by the REL server, derive the specification of the columns across
        /// the typically multiple Arrow RecordBatches.
        /// <example>
        /// Example:
        /// <code>
        ///  "/:output/String",
        ///  "/:output/String/:height/Int64/:mm/:metric",
        ///  "/:output/String/:weight/Float64/:kg/:metric",
        ///  "/:output/String/:weight/String",
        ///  "/:output/String/:weight/Int64/:kg/:metric"
        /// </code>
        /// Return:
        /// <code>
        ///   { "String", "Symbol", "Mixed", "Symbol", "Symbol"}
        /// </code>
        /// </example>
        /// </summary>
        /// <param name="arrowNames">Enumerable names from Arrow RecordBatches</param>
        /// <returns>Expected column specification</returns>
        private static string[] DeriveColumnSpec(IEnumerable<string> arrowNames)
        {
            var columnSpec = new ArrayList();

            foreach (var arrowName in arrowNames)
            {
                var columnList = ColumnListFrom(arrowName);
                foreach (var (field, i) in columnList.WithIndex())
                {
                    if (i >= columnSpec.Count)
                    {
                        columnSpec.Add(IsSymbol(field) ? "Symbol" : field);
                        continue;
                    }

                    if (IsSymbol(field)) // Symbols
                    {
                        if ("Symbol".Equals(columnSpec[i])) continue;
                        columnSpec[i] = "Mixed";
                    }
                    else
                    {
                        if (field.Equals(columnSpec[i])) continue;

                        switch (field)
                        {
                            case "Float64" when "Int64".Equals(columnSpec[i]):
                                columnSpec[i] = "Float64";
                                continue;
                            case "Int64" when "Float64".Equals(columnSpec[i]):
                                continue;
                            default:
                                columnSpec[i] = "Mixed";
                                break;
                        }
                    }
                }
            }

            return (string[])columnSpec.ToArray(typeof(string));
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