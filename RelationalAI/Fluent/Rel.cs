using System;
using System.Collections;
using System.Collections.Generic;

namespace RelationalAI.Fluent
{
    public class Rel
    {
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
        public static IEnumerable<string> ColumnListFrom(string arrowName)
        {
            return String.IsNullOrWhiteSpace(arrowName) ? Array.Empty<string>() : arrowName[9..].Split('/');
        }

        /// <summary>
        /// Determines if supplied string is a REL symbol 
        /// </summary>
        /// <param name="name">Field value or name</param>
        /// <returns>True if REL symbol</returns>
        public static bool IsSymbol(string name)
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
    }
}