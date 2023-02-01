using System;
using System.Linq;
using System.Threading.Tasks;
using FluentAssertions;
using Xunit;

namespace RelationalAI.Test
{
    [Collection("RelationalAI.Test")]
    public class ExecuteTests : UnitTest
    {
        public static string Uuid = Guid.NewGuid().ToString();
        public static string Dbname = $"csharp-sdk-{Uuid}";
        private readonly EngineFixture engineFixture;

        public ExecuteTests(EngineFixture fixture)
        {
            engineFixture = fixture;
        }

        [Fact]
        public async Task ExecuteV1Test()
        {
            var client = CreateClient();

            await engineFixture.CreateEngineWaitAsync();
            engineFixture.Engine.State.Should().Be(EngineStates.Provisioned);
            await client.CreateDatabaseAsync(Dbname, engineFixture.Engine.Name);

            var query = "x, x^2, x^3, x^4 from x in {1; 2; 3; 4; 5}";
            var rsp = await client.ExecuteV1Async(Dbname, engineFixture.Engine.Name, query, true);

            rsp.Aborted.Should().BeFalse();
            var output = rsp.Output;
            output.Should().HaveCount(1);
            var relation = output[0];
            var relKey = relation.RelKey;
            relKey.Name.Should().Be("output");
            relKey.Keys.Should().Equal(new[] { "Int64", "Int64", "Int64" });
            relKey.Values.Should().Equal(new[] { "Int64" });
            var columns = relation.Columns;
            var expected = new[]
            {
                new object[] {1L, 2L, 3L, 4L, 5L},
                new object[] {1L, 4L, 9L, 16L, 25L},
                new object[] {1L, 8L, 27L, 64L, 125L},
                new object[] {1L, 16L, 81L, 256L, 625L}
            };

            columns.Should().Equal(expected, (l, r) => l.SequenceEqual(r));
        }

        public override async Task DisposeAsync()
        {
            var client = CreateClient();

            try
            {
                await client.DeleteDatabaseAsync(Dbname);
            }
            catch (Exception e)
            {
                await Console.Error.WriteLineAsync(e.ToString());
            }
        }
    }
}
