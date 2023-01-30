using RelationalAI.Fluent;
using Xunit;
using Xunit.Abstractions;

namespace RelationalAI.Test.Fluent
{
    public class RelClientBuilderFixture : RelFixtureBase
    {
        public RelClientBuilderFixture(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public void TestInt64()
        {
            var query = "x, x^2, x^3, x^4 from x in {1; 2; 3; 4; 5}";
            RelClientBuilder
                .WithConnection("EY")
                .WithEngine("johann-dev")
                .WithDatabase("johann-test")
                .Query(query)
                .Then(result => result.Results.ToDataTable().Print(toOutput));
        }

        [Fact]
        public void TestInt64FromProfile()
        {
            var query = "x, x^2, x^3, x^4 from x in {1; 2; 3; 4; 5}";
            RelClientBuilder
                .FromProfile("UnitTest")
                .Query(query)
                .Then(result => result.Results.ToDataTable().Print(toOutput));
        }
        
        [Fact]
        public void TestNonSquare()
        {
            const string query =
                @"def output {
                    (:one);
                    (:two, 2);
                    (:three, 3, ""three"")
                }";

            RelClientBuilder
                .FromProfile("UnitTest")
                .Query(query)
                .Then(result => result.Results.ToDataTable().Print(toOutput));
        }

        [Fact]
        public void TestInventoryItems()
        {
            const string query =
                @"def output {
                    (:electronics, ""EAN111-1"", :weight, 10, :kg, :metric);
                    (:electronics, ""EAN111-1"", :weight, 22.05, :lbs, :imperial);
                    (:electronics, ""EAN111-1"", :height, 1000, :mm, :metric);
                    (:electronics, ""EAN111-1"", :height, 39.37, :inch, :imperial);
                    (:hardware, ""EAN222-2"", :weight, 5, :kg, :metric);
                    (:hardware, ""EAN222-2"", :weight, 11.03, :lbs, :imperial);
                    (:hardware, ""EAN222-2"", :height, 725, :mm, :metric);
                    (:hardware, ""EAN222-2"", :height, 28.54, :inch, :imperial);                
                    (:electronics, ""EAN333-3"", :weight, 7.5, :kg, :metric);
                    (:electronics, ""EAN333-3"", :weight, 16.53, :lbs, :imperial);
                    (:electronics, ""EAN333-3"", :height, 125, :mm, :metric);
                    (:electronics, ""EAN333-3"", :height, 4.92, :inch, :imperial)    
                }";

            RelClientBuilder
                .FromProfile("UnitTest")
                .Query(query)
                .Then(result => result.Results.ToDataTable().Print(toOutput));
        }

        [Fact]
        public void TestMixedColumnTypesWithSymbols()
        {
            const string query =
                @"def myValues {
                    (1, :one, ""one"", 1.1);
                    (2, :two, ""two"", 2.2);
                    (3, :three, ""three"", 3.3)
                }";

            RelClientBuilder
                .FromProfile("UnitTest")
                .Query(query)
                .Then(result => result.Results.ToDataTable().Print(toOutput));
        }

        [Fact]
        public void TestSimpleMixed()
        {
            const string query =
                @"def myValues {
                    (1, 1);
                    (2, ""two"");
                    (3, 3.3)
                }";

            RelClientBuilder
                .FromProfile("UnitTest")
                .Query(query)
                .Then(result => result.Results.ToDataTable().Print(toOutput));
        }

        [Fact]
        public void TestSimpleMixedRepeat()
        {
            const string query =
                @"def output {
                    (1, 1);
                    (2, ""two"");
                    (2, ""two+"");
                    (3, ""three"", 3.3)
                }";

            RelClientBuilder
                .FromProfile("UnitTest")
                .Query(query)
                .Then(result => result.Results.ToDataTable().Print(toOutput));
        }

        [Fact]
        public void TestSimpleSymbols()
        {
            const string query =
                @"def output {
                    (:fruit, ""Apple"");
                    (:fruit, ""Orange"");
                    (:veg, ""Carrot"");
                    (:veg, ""Pea"");
                    (:fruit, ""Banana"")
                }";

            RelClientBuilder
                .FromProfile("UnitTest")
                .Query(query)
                .Then(result => result.Results.ToDataTable().Print(toOutput));
        }

        [Fact]
        public void TestOnlySymbols()
        {
            const string query =
                @"def output {
                    (:fruit, :apple);
                    (:fruit, :orange);
                    (:veg, :carrot);
                    (:veg, :pea);
                    (:fruit, :banana)
                }";

            RelClientBuilder
                .FromProfile("UnitTest")
                .Query(query)
                .Then(result => result.Results.ToDataTable().Print(toOutput));
        }

        [Fact]
        public void TestReadModels()
        {
            const string query =
                @"def output:model[name] = rel:catalog:model(name, _)";

            RelClientBuilder
                .FromProfile("UnitTest")
                .Query(query)
                .Then(result => result.Results.ToDataTable().Print(toOutput));
        }

        [Fact]
        public void TestColumnSpec1()
        {
            var l = new[]
            {
                "/:output/String/:height/Float64/:inch/:imperial",
                "/:output/String/:height/Int64/:mm/:metric",
                "/:output/String/:weight/Float64/:kg/:metric",
                "/:output/String/:weight/Float64/:lbs/:imperial",
                "/:output/String/:weight/Int64/:kg/:metric"
            };

            var expected = new[] { "String", "Symbol", "Float64", "Symbol", "Symbol" };
            Assert.Equal(expected, DataTableExtension.DeriveColumnSpec(l));
        }

        [Fact]
        public void TestColumnSpec2()
        {
            var l = new[]
            {
                "/:output/String",
                "/:output/String/:height/Int64/:mm/:metric",
                "/:output/String/:weight/Float64/:kg/:metric",
                "/:output/String/:weight/String",
                "/:output/String/:weight/Int64/:kg/:metric"
            };

            var expected = new[] { "String", "Symbol", "Mixed", "Symbol", "Symbol" };
            Assert.Equal(expected, DataTableExtension.DeriveColumnSpec(l));
        }
    }
}