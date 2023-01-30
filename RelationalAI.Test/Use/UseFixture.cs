using System;
using System.Reflection;
using RelationalAI.Fluent;
using Xunit;
using Xunit.Abstractions;

namespace RelationalAI.Test.Use
{
    public class UseFixture : RelFixtureBase
    {
        public UseFixture(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public void TestAsQuery()
        {
            RelClientBuilder
                .FromProfile("UnitTest")
                .Query(RelQueryBuilder
                    .FromAssembly(Assembly.GetExecutingAssembly())
                    .UseResource("common.rel")
                    .UseResource("journal_entry.rel")
                    .UseResource("journal_entry.test.rel")
                    // .ToBuilder()
                    // .Print(toOutput)
                )
                .Then(result => result.Results.ToDataTable().Print(toOutput))
                .Then(result =>
                {
                    result.Analyzer()
                        .ExpectConstraintViolated("j_entry_debit_credit_test", failWithMessage)
                        .ExpectConstraintViolated("j_entry_fc_amount_test_fail", failWithMessage)
                        .WhenConstraintViolated("j_entry_fc_amount_test", failWithMessage)
                        .WhenConstraintViolated("j_entry_client_account_map_test", failWithMessage);
                });

        }
    }
}