using System;
using System.Threading.Tasks;
using System.Threading;
using Xunit;
using RelationalAI;

namespace RelationalAI.Test
{
    public class EngineFixture : IDisposable
    {
        private Engine _engine;
        private readonly string engineName = "sdk-csharp-" + Guid.NewGuid().ToString();
        // Semaphore is used to lock the CreateEngine function so that only one test creates the engine.
        private static readonly SemaphoreSlim semaphoreSlim = new SemaphoreSlim(1);

        public void Dispose()
        {
            try
            {
                var ut = new UnitTest();
                var client = ut.CreateClient();
                client.DeleteEngineWaitAsync(engineName).Wait();
            }
            catch (Exception e)
            {
                Console.Error.WriteLine(e.ToString());
            }
        }

        public async Task<Engine> CreateEngineWaitAsync()
        {
            try
            {
                if (_engine == null)
                {
                    await semaphoreSlim.WaitAsync();
                    var ut = new UnitTest();
                    var client = ut.CreateClient();
                    _engine = await client.CreateEngineWaitAsync(engineName);
                }
            }
            finally
            {
                semaphoreSlim.Release();
            }


            return _engine;
        }

        public Engine Engine
        {
            get
            {
                return _engine;
            }
        }
    }

    [CollectionDefinition("RelationalAI.Test")]
    public class RelationalAITestCollection : ICollectionFixture<EngineFixture>
    {
        // This class has no code, and is never created. Its purpose is simply
        // to be the place to apply [CollectionDefinition] and all the
        // ICollectionFixture<> interfaces.
    }

}