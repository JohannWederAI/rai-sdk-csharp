using System;
using System.Reflection;
using RelationalAI.Fluent;
using Xunit;
using Xunit.Abstractions;
using Xunit.Sdk;

namespace RelationalAI.Test.Fluent
{
    public class RelQueryBuilderFixture : RelFixtureBase
    {

        public RelQueryBuilderFixture(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public void TestLoadOneResource()
        {
            RelQueryBuilder
                .FromResource(Assembly.GetExecutingAssembly(), "rel.fruit.rel")
                .Print(toOutput);
        }

        [Fact]
        public void TestLoadMultipleResources()
        {
            RelQueryBuilder
                .FromResource(Assembly.GetExecutingAssembly(), "rel.fruit.rel")
                .UseResource(Assembly.GetAssembly(typeof(RelQueryBuilder)), "rel.getmodelnames.rel")
                .ToBuilder()
                .Print(toOutput);
        }

        [Fact]
        public void TestBoatsWithConstraintViolatedQuery()
        {
            RelClientBuilder
                .FromProfile("UnitTest")
                .Query(RelQueryBuilder
                    .FromResource(Assembly.GetExecutingAssembly(), "rel.boats.rel"))
                .Then(result => result.Results.ToDataTable().Print(toOutput))
                .Then(result =>
                {
                    result.Analyzer()
                        .WhenConstraintViolated("boats_for_sale_cardinality", failWithMessage)
                        .ExpectConstraintViolated("eighty_footers", failWithMessage)
                        .ExpectConstraintViolated("only_feet", failWithMessage)
                        .WhenConstraintViolated("positive_values", failWithMessage);
                });
        }

        [Fact]
        public void TestMixingResourcesFromAssemblies()
        {
            RelClientBuilder
                .WithConnection("EY")
                .WithEngine("johann-dev")
                .WithDatabase("johann-test")
                .Query(RelQueryBuilder
                    .FromResource(Assembly.GetExecutingAssembly(), "rel.fruit.rel")
                    .UseResource(Assembly.GetAssembly(typeof(RelQueryBuilder)), "rel.getmodelnames.rel"))
                .Then(r => { r.Results.ToDataTable().Print(toOutput); });
        }
    }
}