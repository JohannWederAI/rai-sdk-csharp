using System;
using System.Collections.Generic;
using Apache.Arrow;
using Apache.Arrow.Types;
using Microsoft.Data.Analysis;

namespace RelationalAI.Fluent
{
  public static class DataFrameExtension
  {
    public static DataFrame ToDataFrame(this List<ArrowRelation> result)
    {
      foreach (var ar in result)
      {
        var dfCol = new ArrowStringDataFrameColumn(ar.RelationId);
      }

      return null;
    }
  }
}
