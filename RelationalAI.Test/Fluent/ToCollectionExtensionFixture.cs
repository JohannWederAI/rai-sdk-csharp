using System;
using System.Collections;
using System.Collections.Generic;
using System.Reflection;
using RelationalAI.Fluent;
using Xunit;
using Xunit.Abstractions;
using Xunit.Sdk;

namespace RelationalAI.Test.Fluent
{
    public class ArrowToListExtensionFixture : RelFixtureBase
    {
        public ArrowToListExtensionFixture(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public void TestToList()
        {
            RelClientBuilder
                .FromProfile("UnitTest")
                .Query(
                    RelQueryBuilder
                        .FromResource(Assembly.GetExecutingAssembly(), "rel.boats.rel"))
                .Then(result => result.Results.ToArrayList().Print(toOutput));
        }
        
        [Fact]
        public void TestToListOfCol()
        {
            RelClientBuilder
                .FromProfile("UnitTest")
                .Query(
                    RelQueryBuilder
                        .FromResource(Assembly.GetExecutingAssembly(), "rel.boats.rel"))
                .Then(result =>
                {
                    var expected = new ArrayList(new[] {"Ferretti", "Bering", "Nordhavn", "San Lorenzo"});
                    var actual = result.Results.ToArrayList(0);
                    Assert.Equal(expected , actual.Print(toOutput));
                })
                .Then(result =>
                {
                    var expected = new ArrayList(new[] {"680", "B77", "N80", "SL70"});
                    var actual = result.Results.ToArrayList(1);
                    Assert.Equal(expected , actual.Print(toOutput));
                })
                .Then(result =>
                {
                    var expected = new ArrayList(new[] {21.7, 85, 82, 70});
                    var actual = result.Results.ToArrayList(2);
                    //Assert.Equal(expected , actual.Print(toOutput));
                })
                .Then(result =>
                {
                    var expected = new ArrayList(new[] {":m", ":ft", ":ft", ":ft"});
                    var actual = result.Results.ToArrayList(3);
                    Assert.Equal(expected , actual.Print(toOutput));
                });
        }
        
        [Fact]
        public void TestToListOfString()
        {
            RelClientBuilder
                .FromProfile("UnitTest")
                .Query(
                    RelQueryBuilder
                        .FromResource(Assembly.GetExecutingAssembly(), "rel.boats.rel"))
                .Then(result => result.Results.ToList<string>().Print(toOutput));
        }
        
        [Fact]
        public void TestToListOfDouble()
        {
            RelClientBuilder
                .FromProfile("UnitTest")
                .Query(
                    RelQueryBuilder
                        .FromResource(Assembly.GetExecutingAssembly(), "rel.boats.rel"))
                .Then(result => result.Results.ToList<double>(2).Print(toOutput));
        }
        
        [Fact]
        public void TestToListOfInt()
        {
            RelClientBuilder
                .FromProfile("UnitTest")
                .Query(
                    RelQueryBuilder
                        .FromResource(Assembly.GetExecutingAssembly(), "rel.boats.rel"))
                .Then(result => result.Results.ToList<int>(2).Print(toOutput));
        }
        
        [Fact]
        public void TestToSetOfString()
        {
            RelClientBuilder
                .FromProfile("UnitTest")
                .Query(
                    RelQueryBuilder
                        .FromResource(Assembly.GetExecutingAssembly(), "rel.boats.rel"))
                .Then(result => result.Results.ToSet<string>().Print(toOutput));
        }
        
        [Fact]
        public void TestToSetOfDouble()
        {
            RelClientBuilder
                .FromProfile("UnitTest")
                .Query(
                    RelQueryBuilder
                        .FromResource(Assembly.GetExecutingAssembly(), "rel.boats.rel"))
                .Then(result => result.Results.ToSet<double>(2).Print(toOutput));
        }
    }
}