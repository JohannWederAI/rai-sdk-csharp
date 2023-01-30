using System;
using RelationalAI.Fluent;
using Xunit;
using Xunit.Abstractions;
using Xunit.Sdk;

namespace RelationalAI.Test
{

    public class ClientFixture : IDisposable
    {
        protected readonly ITestOutputHelper _output;
        protected Action<string> toOutput;

        protected ClientFixture(ITestOutputHelper output)
        {
            _output = output;
            toOutput = (l) => _output.WriteLine(l);
        }

        public void Dispose()
        {
        }

        [Fact]
        public void TestListModelsAsync()
        {
            RelClientBuilder
                .FromProfile("UnitTest")
                .WithModels(toOutput);
        }
    }
}