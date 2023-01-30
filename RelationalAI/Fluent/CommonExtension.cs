using System.Collections.Generic;
using System.Linq;

namespace RelationalAI.Fluent
{
    public static class CommonExtension
    {
        /// <summary>
        /// Provides support for indexed foreach.
        /// <example>
        /// Example:
        ///   <code>foreach (var (item, i) in myEnumerable.WithIndex())</code>
        /// </example>
        /// </summary>
        /// <param name="source">Any enumerable.</param>
        /// <typeparam name="T">The type of item.</typeparam>
        /// <returns>Indexed tuple enumerable.</returns>
        public static IEnumerable<(T item, int index)> WithIndex<T>(this IEnumerable<T> source)
        {
            return source.Select((item, i) => (item, i));
        }
    }
}