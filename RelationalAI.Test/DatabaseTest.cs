using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Xunit;

namespace RelationalAI.Test
{
    [Collection("RelationalAI.Test")]
    public class DatabaseTests : UnitTest
    {
        private readonly EngineFixture engineFixture;
        public static string Uuid = Guid.NewGuid().ToString();
        public static string Dbname = $"csharp-sdk-{Uuid}";

        public DatabaseTests(EngineFixture fixture)
        {
            engineFixture = fixture;
        }

        [Fact]
        public async Task DatabaseTest()
        {
            var client = CreateClient();
            await engineFixture.CreateEngineWaitAsync();

            await Assert.ThrowsAsync<HttpError>(async () => await client.DeleteDatabaseAsync(Dbname));

            var createRsp = await client.CreateDatabaseAsync(Dbname, engineFixture.Engine.Name, false);
            Assert.Equal(Dbname, createRsp.Name);
            Assert.Equal(DatabaseState.Created, createRsp.State);

            createRsp = await client.CreateDatabaseAsync(Dbname, engineFixture.Engine.Name, true);
            Assert.Equal(Dbname, createRsp.Name);
            Assert.Equal(DatabaseState.Created, createRsp.State);

            var database = await client.GetDatabaseAsync(Dbname);
            Assert.Equal(Dbname, database.Name);
            Assert.Equal(DatabaseState.Created, database.State);

            var databases = await client.ListDatabasesAsync();
            database = databases.Find(db => db.Name == Dbname);
            Assert.Equal(Dbname, database.Name);
            Assert.Equal(DatabaseState.Created, database.State);

            databases = await client.ListDatabasesAsync(DatabaseState.Created);
            database = databases.Find(db => db.Name == Dbname);
            Assert.Equal(Dbname, database.Name);
            Assert.Equal(DatabaseState.Created, database.State);

            await Assert.ThrowsAsync<ArgumentOutOfRangeException>(() =>
                client.ListDatabasesAsync((DatabaseState)1000));

            var edbs = await client.ListEdbsAsync(Dbname, engineFixture.Engine.Name);
            var edb = edbs.Find(item => item.Name.Equals("rel"));
            Assert.NotNull(edb);
            
            var deleteRsp = await client.DeleteDatabaseAsync(Dbname);
            Assert.Equal(Dbname, deleteRsp.Name);

            await Assert.ThrowsAsync<HttpError>(async () => await client.GetDatabaseAsync(Dbname));
        }

        private readonly Dictionary<string, string> TestModel = new Dictionary<string, string> { { "test_model", "def R = \"hello\", \"world\"" } };

        private const string TestJson = "{" +
                                        "\"name\":\"Amira\",\n" +
                                        "\"age\":32,\n" +
                                        "\"height\":null,\n" +
                                        "\"pets\":[\"dog\",\"rabbit\"]}";

        [Fact]
        public async Task DatabaseCloneTest()
        {
            var client = CreateClient();

            await engineFixture.CreateEngineWaitAsync();
            await Assert.ThrowsAsync<HttpError>(async () => await client.DeleteDatabaseAsync(Dbname));

            // create a fresh database
            var createRsp = await client.CreateDatabaseAsync(Dbname, engineFixture.Engine.Name);
            Assert.Equal(Dbname, createRsp.Name);
            Assert.Equal(DatabaseState.Created, createRsp.State);

            // load some data and model
            var loadRsp = await client.LoadJsonAsync(Dbname, engineFixture.Engine.Name, "test_data", TestJson);
            Assert.False(loadRsp.Aborted);
            Assert.Empty(loadRsp.Output);
            Assert.Empty(loadRsp.Problems);

            var resp = await client.LoadModelsWaitAsync(Dbname, engineFixture.Engine.Name, TestModel);
            Assert.Equal(TransactionAsyncState.Completed, resp.Transaction.State);
            Assert.Empty(resp.Problems);

            // clone database
            var databaseCloneName = $"{Dbname}-clone";
            createRsp = await client.CloneDatabaseAsync(databaseCloneName, engineFixture.Engine.Name, Dbname, true);
            Assert.Equal(databaseCloneName, createRsp.Name);
            Assert.Equal(DatabaseState.Created, createRsp.State);

            // make sure the database exists
            var database = await client.GetDatabaseAsync(databaseCloneName);
            Assert.Equal(databaseCloneName, database.Name);
            Assert.Equal(DatabaseState.Created, database.State);

            var databases = await client.ListDatabasesAsync();
            database = databases.Find(db => db.Name == databaseCloneName);
            Assert.NotNull(database);
            Assert.Equal(databaseCloneName, database.Name);
            Assert.Equal(DatabaseState.Created, database.State);

            // make sure the data was cloned
            var rsp = await client.ExecuteV1Async(databaseCloneName, engineFixture.Engine.Name, "test_data", true);

            var rel = FindRelation(rsp.Output, ":name");
            Assert.NotNull(rel);

            rel = FindRelation(rsp.Output, ":age");
            Assert.NotNull(rel);

            rel = FindRelation(rsp.Output, ":height");
            Assert.NotNull(rel);

            rel = FindRelation(rsp.Output, ":pets");
            Assert.NotNull(rel);

            // cleanup
            var deleteRsp = await client.DeleteDatabaseAsync(databaseCloneName);
            Assert.Equal(databaseCloneName, deleteRsp.Name);
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
