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
            var columnSpec = Rel.DeriveColumnSpec(arrowRelations.Select(r => r.RelationId));

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
            var columnList = Rel.ColumnListFrom(arrowRelation.RelationId);
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

            return dataRow;
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