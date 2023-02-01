using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using RelationalAI.Fluent;
using Xunit;

namespace RelationalAI.Test
{
    [Collection("RelationalAI.Test")]
    public class ModelsTests : UnitTest
    {
        public static string Uuid = Guid.NewGuid().ToString();
        public static string Dbname = $"csharp-sdk-{Uuid}";
        private readonly Dictionary<string, string> TestModel = new Dictionary<string, string> { { "test_model", "def R = \"hello\", \"world\"" } };

        private readonly EngineFixture engineFixture;

        public ModelsTests(EngineFixture fixture)
        {
            engineFixture = fixture;
        }

        [Fact]
        public async Task ModelsTest()
        {
            var builder = RelClientBuilder.FromProfile("UnitTest");
            var client = builder.ToClient();

            var resp = await client.LoadModelsWaitAsync(builder.DatabaseName, builder.EngineName, TestModel);
            Assert.Equal(TransactionAsyncState.Completed, resp.Transaction.State);
            Assert.Empty(resp.Problems);

            builder.Query(RelQueryBuilder.GetModel("test_model")).Then(r =>
            {
                Assert.Equal(TestModel["test_model"], r.Results.ToList<string>().Single());
            });
            
            builder.Query(RelQueryBuilder.GetModels()).Then(r =>
            {
                Assert.Contains("test_model", r.Results.ToList<string>());
            });
            
            var deleteRsp = await client.DeleteModelsAsync(builder.DatabaseName, builder.EngineName, new List<string> { "test_model" });
            Assert.Equal(TransactionAsyncState.Completed, deleteRsp.Transaction.State);
            Assert.Empty(deleteRsp.Problems);

            builder.Query(RelQueryBuilder.GetModels()).Then(r =>
            {
                Assert.DoesNotContain("test_model", r.Results.ToList<string>());
            });
        }

        public override async Task DisposeAsync()
        {
        }
    }
}
