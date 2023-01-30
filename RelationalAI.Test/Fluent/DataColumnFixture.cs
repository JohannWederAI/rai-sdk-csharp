using System;
using System.Text;
using System.Text.Json;
using Xunit;
using Xunit.Abstractions;

namespace RelationalAI.Test.Fluent
{
    public class DataColumnFixture
    {
        private readonly ITestOutputHelper _testOutputHelper;
        
        public DataColumnFixture(ITestOutputHelper testOutputHelper)
        {
            _testOutputHelper = testOutputHelper;
            
        }
        
        [Fact]
        public void TestOutput()
        {
            _testOutputHelper.WriteLine("Hello");
        }
        
        private class RelationInfo 
        {
            public long relation_count { get; set; }
        }

        [Fact]
        public void TestInt64()
        {
            var bytes = Convert.FromBase64String("eyJyZWxhdGlvbl9jb3VudCI6IDN9");
            _testOutputHelper.WriteLine(Encoding.UTF8.GetString(bytes));
            _testOutputHelper.WriteLine(Encoding.ASCII.GetString(bytes));
            var jsonString = Encoding.UTF8.GetString(bytes);
            var info = JsonSerializer.Deserialize<RelationInfo>(jsonString); 
            _testOutputHelper.WriteLine(info.relation_count.ToString());
        }
        
        [Fact]
        public void TestNonSquare()
        {
            var bytes = Convert.FromBase64String(
                "/////6gAAAAQAAAAAAAKAAwACgAIAAQACgAAABAAAAABAAQACAAIAAAABAAIAAAABAAAAAIAAABEAAAABAAAANT///8QAAAAEAAAAAAAAgAUAAAAAAAAAMT///8AAAABQAAAAAIAAAB2MgAAEAAUABAAAAAOAAgAAAAEABAAAAAQAAAAGAAAAAAAAgAcAAAAAAAAAAgADAAIAAcACAAAAAAAAAFAAAAAAgAAAHYxAAD/////uAAAABQAAAAAAAAADAAWABQAEgAMAAQADAAAABAAAAAAAAAAFAAAAAAAAwAEAAoAGAAMAAgABAAKAAAAFAAAAFgAAAABAAAAAAAAAAAAAAAEAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAACAAAAAAAAAAIAAAAAAAAAAAAAAAAAAAACAAAAAAAAAAIAAAAAAAAAAAAAAACAAAAAQAAAAAAAAAAAAAAAAAAAAEAAAAAAAAAAAAAAAAAAAABAAAAAAAAAAEAAAAAAAAA/////wAAAAA=");
            _testOutputHelper.WriteLine(Encoding.UTF8.GetString(bytes));
        }
        
        [Fact]
        public void TestMixedColumnsWithSymbols()
        {

        }
        
        [Fact]
        public void TestSimpleMixed()
        {

        }
        
        [Fact]
        public void TestSimpleMixedRepeat()
        {

        }

 
    }
}


