using System;
using Xunit.Abstractions;
using Xunit.Sdk;

namespace RelationalAI.Test
{
    public abstract class RelFixtureBase : IDisposable
    {
        protected readonly ITestOutputHelper _output;
        protected Action<TransactionAsyncResult, string> failWithMessage;
        protected Action<string> toOutput;

        protected RelFixtureBase(ITestOutputHelper output)
        {
            _output = output;
            failWithMessage = (result, s) => throw new XunitException(s);
            toOutput = (l) => _output.WriteLine(l);
            toOutput("Results:");
        }

        public void Dispose()
        {
            toOutput("Done.");
        }
    }
}