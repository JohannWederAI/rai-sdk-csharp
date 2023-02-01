using System;
using System.Linq;
using System.Threading.Tasks;
using FluentAssertions;
using Xunit;

namespace RelationalAI.Test
{
    [Collection("RelationalAI.Test")]
    public class LoadJsonTests : UnitTest
    {
        public static string Uuid = Guid.NewGuid().ToString();
        public static string Dbname = $"csharp-sdk-{Uuid}";
        private const string Sample = "{" +
                                      "\"name\":\"Amira\",\n" +
                                      "\"age\":32,\n" +
                                      "\"height\":null,\n" +
                                      "\"pets\":[\"dog\",\"rabbit\"]}";

        private readonly EngineFixture engineFixture;

        public LoadJsonTests(EngineFixture fixture)
        {
            engineFixture = fixture;
        }

        [Fact]
        public async Task LoadJsontTest()
        {
            var client = CreateClient();

            await engineFixture.CreateEngineWaitAsync();
            engineFixture.Engine.State.Should().Be(EngineStates.Provisioned);

            await client.CreateDatabaseAsync(Dbname, engineFixture.Engine.Name);

            var loadRsp = await client.LoadJsonAsync(Dbname, engineFixture.Engine.Name, "sample", Sample);
            loadRsp.Aborted.Should().BeFalse();
            loadRsp.Output.Should().HaveCount(0);
            loadRsp.Problems.Should().HaveCount(0);

            var rsp = await client.ExecuteV1Async(Dbname, engineFixture.Engine.Name, "def output = sample");

            var rel = FindRelation(rsp.Output, ":name");
            rel.Should().NotBeNull();
            rel.Columns.Should().HaveCount(1);
            rel.Columns.Should().Equal(new[] { new object[] { "Amira" } },
                (l, r) => l.SequenceEqual(r)
            );

            rel = FindRelation(rsp.Output, ":age");
            rel.Should().NotBeNull();
            rel.Columns.Should().HaveCount(1);
            rel.Columns.Should().Equal(new[] { new object[] { 32L } },
                (l, r) => l.SequenceEqual(r)
            );

            rel = FindRelation(rsp.Output, ":height");
            rel.Should().NotBeNull();
            rel.Columns.Should().HaveCount(1);
            rel.Columns.Should().Equal(new[] { new object[] { null } },
                (l, r) => l.SequenceEqual(r)
            );

            rel = FindRelation(rsp.Output, ":pets");
            rel.Should().NotBeNull();
            rel.Columns.Length.Should().Be(2);
            rel.Columns.Should().Equal(new[] { new object[] { 1L, 2L }, new object[] { "dog", "rabbit" } },
                (l, r) => l.SequenceEqual(r)
            );
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
