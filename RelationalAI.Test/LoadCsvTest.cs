using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using System.Linq;
using FluentAssertions;
using Xunit;

namespace RelationalAI.Test
{
    [Collection("RelationalAI.Test")]
    public class LoadCsvTests : UnitTest
    {
        public static string Uuid = Guid.NewGuid().ToString();
        public static string Dbname = $"csharp-sdk-{Uuid}";

        private const string Sample = "" +
                                      "cocktail,quantity,price,date\n" +
                                      "\"martini\",2,12.50,\"2020-01-01\"\n" +
                                      "\"sazerac\",4,14.25,\"2020-02-02\"\n" +
                                      "\"cosmopolitan\",4,11.00,\"2020-03-03\"\n" +
                                      "\"bellini\",3,12.25,\"2020-04-04\"\n";

        private readonly EngineFixture engineFixture;

        public LoadCsvTests(EngineFixture fixture)
        {
            engineFixture = fixture;
        }

        [Fact]
        public async Task LoadCsvtTest()
        {
            var client = CreateClient();

            await engineFixture.CreateEngineWaitAsync();
            engineFixture.Engine.State.Should().Be(EngineStates.Provisioned);
            await client.CreateDatabaseAsync(Dbname, engineFixture.Engine.Name);

            var loadRsp = await client.LoadCsvAsync(Dbname, engineFixture.Engine.Name, "sample", Sample);
            loadRsp.Aborted.Should().BeFalse();
            loadRsp.Output.Should().HaveCount(0);
            loadRsp.Problems.Should().HaveCount(0);

            var rsp = await client.ExecuteV1Async(Dbname, engineFixture.Engine.Name, "def output = sample");

            var rel = FindRelation(rsp.Output, ":date");
            rel.Should().NotBeNull();
            rel.Columns.Length.Should().Be(2);

            rel.Columns.Should().Equal(new[]
                {
                    new object[] {2L, 3L, 4L, 5L},
                    new object[] {"2020-01-01", "2020-02-02", "2020-03-03", "2020-04-04"}
                },
                (l, r) => l.SequenceEqual(r)
            );

            rel = FindRelation(rsp.Output, ":price");
            rel.Should().NotBeNull();
            rel.Columns.Length.Should().Be(2);

            rel.Columns.Should().Equal(new[]
                {
                    new object[] {2L, 3L, 4L, 5L},
                    new object[] {"12.50", "14.25", "11.00", "12.25"}
                },
                (l, r) => l.SequenceEqual(r)
            );

            rel = FindRelation(rsp.Output, ":quantity");
            rel.Should().NotBeNull();
            rel.Columns.Length.Should().Be(2);

            rel.Columns.Should().Equal(new[]
                {
                    new object[] {2L, 3L, 4L, 5L},
                    new object[] {"2", "4", "4", "3"}
                },
                (l, r) => l.SequenceEqual(r)
             );

            rel = FindRelation(rsp.Output, ":cocktail");
            rel.Should().NotBeNull();
            rel.Columns.Length.Should().Be(2);
            rel.Columns.Should().Equal(new[]
                {
                    new object[] {2L, 3L, 4L, 5L},
                    new object[] {"martini", "sazerac", "cosmopolitan", "bellini"}
                },
                (l, r) => l.SequenceEqual(r)
            );
        }

        private const string SampleNoHeader = "" +
                                              "\"martini\",2,12.50,\"2020-01-01\"\n" +
                                              "\"sazerac\",4,14.25,\"2020-02-02\"\n" +
                                              "\"cosmopolitan\",4,11.00,\"2020-03-03\"\n" +
                                              "\"bellini\",3,12.25,\"2020-04-04\"\n";

        [Fact]
        public async Task LoadCsvNoHeaderTest()
        {
            var client = CreateClient();

            await engineFixture.CreateEngineWaitAsync();
            engineFixture.Engine.State.Should().Be(EngineStates.Provisioned);

            await client.CreateDatabaseAsync(Dbname, engineFixture.Engine.Name);

            var opts = new CsvOptions().WithHeaderRow(0);
            var loadRsp = await client.LoadCsvAsync(Dbname, engineFixture.Engine.Name, "sample_no_header", SampleNoHeader, opts);
            loadRsp.Aborted.Should().BeFalse();
            loadRsp.Output.Should().HaveCount(0);
            loadRsp.Problems.Should().HaveCount(0);

            var rsp = await client.ExecuteV1Async(Dbname, engineFixture.Engine.Name, "def output = sample_no_header");

            var rel = FindRelation(rsp.Output, ":COL1");
            rel.Should().NotBeNull();
            rel.Columns.Length.Should().Be(2);
            rel.Columns.Should().Equal(new[]
                {
                    new object[] {1L, 2L, 3L, 4L},
                    new object[] {"martini", "sazerac", "cosmopolitan", "bellini"}
                },
                (l, r) => l.SequenceEqual(r)
            );

            rel = FindRelation(rsp.Output, ":COL2");
            rel.Should().NotBeNull();
            rel.Columns.Length.Should().Be(2);
            rel.Columns.Should().Equal(new[]
                {
                    new object[] {1L, 2L, 3L, 4L},
                    new object[] {"2", "4", "4", "3"}
                },
                (l, r) => l.SequenceEqual(r)
             );

            rel = FindRelation(rsp.Output, ":COL3");
            rel.Should().NotBeNull();
            rel.Columns.Length.Should().Be(2);
            rel.Columns.Should().Equal(new[]
                {
                    new object[] {1L, 2L, 3L, 4L},
                    new object[] {"12.50", "14.25", "11.00", "12.25"}
                },
                (l, r) => l.SequenceEqual(r)
            );

            rel = FindRelation(rsp.Output, ":COL4");
            rel.Should().NotBeNull();
            rel.Columns.Length.Should().Be(2);

            rel.Columns.Should().Equal(new[]
                {
                    new object[] {1L, 2L, 3L, 4L},
                    new object[] {"2020-01-01", "2020-02-02", "2020-03-03", "2020-04-04"}
                },
                (l, r) => l.SequenceEqual(r)
            );
        }

        private const string SampleAltSyntax = "" +
                                               "cocktail|quantity|price|date\n" +
                                               "'martini'|2|12.50|'2020-01-01'\n" +
                                               "'sazerac'|4|14.25|'2020-02-02'\n" +
                                               "'cosmopolitan'|4|11.00|'2020-03-03'\n" +
                                               "'bellini'|3|12.25|'2020-04-04'\n";

        [Fact]
        public async Task LoadCsvAltSyntaxTest()
        {
            var client = CreateClient();

            await engineFixture.CreateEngineWaitAsync();
            engineFixture.Engine.State.Should().Be(EngineStates.Provisioned);

            await client.CreateDatabaseAsync(Dbname, engineFixture.Engine.Name);

            var opts = new CsvOptions().WithDelim('|').WithQuoteChar('\'');
            var loadRsp = await client.LoadCsvAsync(Dbname, engineFixture.Engine.Name, "sample_alt_syntax", SampleAltSyntax, opts);
            loadRsp.Aborted.Should().BeFalse();
            loadRsp.Output.Should().HaveCount(0);
            loadRsp.Problems.Should().HaveCount(0);

            var rsp = await client.ExecuteV1Async(Dbname, engineFixture.Engine.Name, "def output = sample_alt_syntax");

            var rel = FindRelation(rsp.Output, ":date");
            rel.Should().NotBeNull();
            rel.Columns.Length.Should().Be(2);
            rel.Columns.Should().Equal(new[]
                {
                    new object[] {2L, 3L, 4L, 5L},
                    new object[] {"2020-01-01", "2020-02-02", "2020-03-03", "2020-04-04"}
                },
                (l, r) => l.SequenceEqual(r)
            );

            rel = FindRelation(rsp.Output, ":price");
            rel.Should().NotBeNull();
            rel.Columns.Length.Should().Be(2);
            rel.Columns.Should().Equal(new[]
                {
                    new object[] {2L, 3L, 4L, 5L},
                    new object[] {"12.50", "14.25", "11.00", "12.25"}
                },
                (l, r) => l.SequenceEqual(r)
            );

            rel = FindRelation(rsp.Output, ":quantity");
            rel.Should().NotBeNull();
            rel.Columns.Length.Should().Be(2);
            rel.Columns.Should().Equal(new[]
                {
                    new object[] {2L, 3L, 4L, 5L},
                    new object[] {"2", "4", "4", "3"}
                },
                (l, r) => l.SequenceEqual(r)
            );

            rel = FindRelation(rsp.Output, ":cocktail");
            rel.Should().NotBeNull();
            rel.Columns.Length.Should().Be(2);
            rel.Columns.Should().Equal(new[]
                {
                    new object[] {2L, 3L, 4L, 5L},
                    new object[] {"martini", "sazerac", "cosmopolitan", "bellini"}
                },
                (l, r) => l.SequenceEqual(r)
            );
        }

        [Fact]
        public async Task LoadCsvWithSchemaTest()
        {
            var client = CreateClient();

            await engineFixture.CreateEngineWaitAsync();
            engineFixture.Engine.State.Should().Be(EngineStates.Provisioned);

            await client.CreateDatabaseAsync(Dbname, engineFixture.Engine.Name);

            var schema = new Dictionary<string, string>
            {
                { "cocktail", "string" },
                { "quantity", "int" },
                { "price", "decimal(64,2)" },
                { "date", "date" }
            };

            var opts = new CsvOptions().WithSchema(schema);
            var loadRsp = await client.LoadCsvAsync(Dbname, engineFixture.Engine.Name, "sample", Sample, opts);
            loadRsp.Aborted.Should().BeFalse();
            loadRsp.Output.Should().HaveCount(0);
            loadRsp.Problems.Should().HaveCount(0);

            var rsp = await client.ExecuteV1Async(Dbname, engineFixture.Engine.Name, "def output = sample");

            var rel = FindRelation(rsp.Output, ":date");
            rel.Should().NotBeNull();
            rel.Columns.Length.Should().Be(2);
            rel.Columns.Should().Equal(new[]
                {
                    new object[] {2L, 3L, 4L, 5L},
                    new object[] {"2020-01-01", "2020-02-02", "2020-03-03", "2020-04-04"}
                },
                (l, r) => l.SequenceEqual(r)
            );

            rel.RelKey.Values.Should().HaveCount(1);
            rel.RelKey.Values[0].Should().Be("Dates.Date");
            rel = FindRelation(rsp.Output, ":price");
            rel.Should().NotBeNull();
            rel.Columns.Length.Should().Be(2);
            rel.Columns.Should().Equal(new[]
                {
                    new object[] {2L, 3L, 4L, 5L},
                    new object[] {12.5, 14.25, 11.00, 12.25}
                },
                (l, r) => l.SequenceEqual(r)
            );
            rel.RelKey.Values.Should().HaveCount(1);
            rel.RelKey.Values[0].Should().Be("FixedPointDecimals.FixedDecimal{Int64, 2}");

            rel = FindRelation(rsp.Output, ":quantity");
            rel.Should().NotBeNull();
            rel.Columns.Length.Should().Be(2);
            rel.Columns.Should().Equal(new[]
                {
                    new object[] {2L, 3L, 4L, 5L},
                    new object[] {2L, 4L, 4L, 3L}
                },
                (l, r) => l.SequenceEqual(r)
            );
            rel.RelKey.Values.Should().HaveCount(1);
            rel.RelKey.Values[0].Should().Be("Int64");

            rel = FindRelation(rsp.Output, ":cocktail");
            rel.Should().NotBeNull();
            rel.Columns.Length.Should().Be(2);
            rel.Columns.Should().Equal(new[]
                {
                    new object[] {2L, 3L, 4L, 5L},
                    new object[] {"martini", "sazerac", "cosmopolitan", "bellini"}
                },
                (l, r) => l.SequenceEqual(r)
            );
            rel.RelKey.Values.Should().HaveCount(1);
            rel.RelKey.Values[0].Should().Be("String");
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
