using System;
using System.Collections;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text;
using Microsoft.Data.Analysis;

namespace RelationalAI.Fluent
{
    public static class DataTableExtension
    {
        /// <summary>
        /// Transforms enumerable <c>ArrowRelation</c> to <c>DataTable</c>. Use with Results from REL server.
        /// <example>
        /// <code>tx.Results.ToDataTable()</code>
        /// </example>
        /// </summary>
        /// <param name="arrowRelations">From REL server response</param>
        /// <returns>DataTable</returns>
        public static DataTable ToDataTable(this IEnumerable<ArrowRelation> arrowRelations)
        {
            var outputRelations = arrowRelations.Where(r => r.RelationId.StartsWith("/:output/"));
            if (!outputRelations.Any())
            {
                return new DataTable();
            }

            var dataTable = CreateDataTableFrom(outputRelations);

            dataTable.BeginLoadData();
            foreach (var arrowRelation in outputRelations)
            {
                var dataFrame = DataFrame.FromArrowRecordBatch(arrowRelation.Table);
                if (!dataFrame.Rows.Any()) // Special case where leading columns are only symbols
                {
                    var dataRow = CreateNewRowFrom(arrowRelation, dataTable);
                    dataTable.Rows.Add(dataRow);
                }
                else
                {
                    foreach (var r in dataFrame.Rows)
                    {
                        var dataRow = CreateNewRowFrom(arrowRelation, dataTable, r);
                        dataTable.Rows.Add(dataRow);
                    }
                }
            }

            dataTable.EndLoadData();

            return dataTable;
        }

        private static DataTable CreateDataTableFrom(IEnumerable<ArrowRelation> arrowRelations)
        {
            var dataTable = new DataTable(":output");
            var columnSpec = DeriveColumnSpec(arrowRelations.Select(r => r.RelationId));

            foreach (var (c, i) in columnSpec.WithIndex())
            {
                var column = new DataColumn();
                column.Caption = c;
                switch (c)
                {
                    case "Int64":
                        column.DataType = System.Type.GetType($"System.Int64");
                        break;
                    case "Float64":
                        column.DataType = System.Type.GetType($"System.Double");
                        break;
                    default:
                        column.DataType = System.Type.GetType($"System.String");
                        break;
                }

                dataTable.Columns.Add(column);
            }

            return dataTable;
        }

        private static DataRow CreateNewRowFrom(ArrowRelation arrowRelation,
            DataTable dataTable, IEnumerable<object> values = null)
        {
            values ??= new string[] { };
            var dataRow = dataTable.NewRow();
            var columnList = ColumnListFrom(arrowRelation.RelationId);
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

            return dataRow;
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
        /// Determines if supplied string is a REL symbol 
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
        public static string[] DeriveColumnSpec(IEnumerable<string> arrowNames)
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

        /// <summary>
        /// Outputs the contents of the DataTable, row to line to the provided writeln Action. The Print methods is
        /// often useful in unit tests and some unit test like xUnit are multi-threaded and have special output
        /// mechanisms instead of Console.
        /// <example>
        /// Simple example:
        /// <code>myDataTable.Print(l => Console.WriteLn(l))</code>
        /// </example>
        /// </summary>
        /// <param name="dataTable">The DataTable to print</param>
        /// <param name="writeln">Mechanism to print lines, probably Console</param>
        /// <param name="spacing">Space between Columns, defaults to 3</param>
        /// <returns>DataTable for convenient assignment</returns>
        public static DataTable Print(this DataTable dataTable, Action<string> writeln, int spacing = 3)
        {
            var colWidths = new Dictionary<string, int>();

            var sb = new StringBuilder();
            foreach (DataColumn col in dataTable.Columns)
            {
                var heading = col.Caption;
                sb.Append(heading);
                var maxLabelSize = dataTable.Rows.OfType<DataRow>()
                    .Select(m => Math.Max((m.Field<object>(col.ColumnName)?.ToString() ?? "").Length, heading.Length))
                    .OrderByDescending(m => m).FirstOrDefault();

                colWidths.Add(col.ColumnName, maxLabelSize);
                for (var i = 0; i < maxLabelSize - heading.Length + spacing; i++) sb.Append(' ');
            }

            writeln(sb.ToString());

            foreach (DataRow dataRow in dataTable.Rows)
            {
                sb = new StringBuilder();
                for (var j = 0; j < dataRow.ItemArray.Length; j++)
                {
                    sb.Append(dataRow.ItemArray[j]);
                    for (var i = 0;
                         i < colWidths[dataTable.Columns[j].ColumnName] - dataRow.ItemArray[j]?.ToString()?.Length +
                         spacing;
                         i++) sb.Append(' ');
                }

                writeln(sb.ToString());
            }

            return dataTable;
        }
    }
}