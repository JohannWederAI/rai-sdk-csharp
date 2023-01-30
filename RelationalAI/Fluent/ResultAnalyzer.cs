using System;
using System.Linq;

namespace RelationalAI.Fluent
{
    public static class TransactionAsyncResultExtension
    {
        public static ResultAnalyzer Analyzer(this TransactionAsyncResult result)
        {
            return new ResultAnalyzer(result);
        }
    }

    public class ResultAnalyzer
    {
        private readonly TransactionAsyncResult _result;

        public ResultAnalyzer(TransactionAsyncResult result)
        {
            this._result = result;
        }

        public ResultAnalyzer WhenConstraintViolated(string constraint, Action<TransactionAsyncResult, string> onViolation)
        {
            foreach (var problem in _result.Problems.OfType<IntegrityConstraintViolation>())
            {
                foreach (var source in problem.Sources)
                {
                    if (source.RelKey.Keys.Any(key =>
                            key.Contains("#" + constraint + "#", StringComparison.InvariantCultureIgnoreCase)))
                    {
                        onViolation(_result, $"Unexpected constraint violation: {constraint}");
                        return this;
                    }
                }
            }

            return this;
        }

        public ResultAnalyzer ExpectConstraintViolated(string constraint,
            Action<TransactionAsyncResult, string> onMissingViolation)
        {
            var foundViolation = false;
            WhenConstraintViolated(constraint, (r, m) => { foundViolation = true; });

            if (!foundViolation)
            {
                onMissingViolation(_result, $"Missing expected constraint violation: {constraint}");
            }

            return this;
        }
    }
}