using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Runtime.CompilerServices;
using System.Text;
using Microsoft.Data.Analysis;
using RelationalAI.Fluent.Exceptions;

namespace RelationalAI.Fluent
{
    public interface INeedDatabase
    {
        RelClientBuilder WithDatabase(string name);
    }

    public interface INeedEngine
    {
        INeedDatabase WithEngine(string name);
    }

    public class RelClientBuilder : INeedEngine, INeedDatabase
    {
        private Client? _client;
        private Dictionary<string, object>? _config;
        private string? _engineName;
        private Engine? _engine;
        private string? _databaseName;
        private Database? _database;
        private TransactionAsyncResult? _result;

        private static RelClientBuilder CreateBuilderFrom(string profile)
        {
            var instance = new RelClientBuilder();
            if (File.Exists(Config.GetRaiConfigPath()))
            {
                instance._config = Config.Read(profile: profile);
            }
            else
            {
                throw new RelClientConfigException("Rel config file not found.");
            }

            var ctx = new Client.Context(instance._config);
            instance._client = new Client(ctx);
            return instance;
        }

        public static INeedEngine WithConnection(string profile = "default")
        {
            return CreateBuilderFrom(profile);
        }

        public static RelClientBuilder FromProfile(string profile = "default")
        {
            var builder = CreateBuilderFrom(profile);
            var engine = builder._config["engine"].ToString();
            var database = builder._config["database"].ToString();
            return builder
                .WithEngine(engine)
                .WithDatabase(database);
        }

        public INeedDatabase WithEngine(string name)
        {
            _engineName = name;
            // _engine = _client.GetEngineAsync(name).GetAwaiter().GetResult();
            return this;
        }

        public RelClientBuilder WithDatabase(string name)
        {
            _databaseName = name;
            // _database = _client.GetDatabaseAsync(name).GetAwaiter().GetResult();
            return this;
        }

        public RelClientBuilder Query(object query)
        {
            var queryStr = query.ToString();
            _result = _client.ExecuteWaitAsync(_databaseName, _engineName, queryStr, true).GetAwaiter().GetResult();
            return this;
        }

        public RelClientBuilder Query(object query, Action<TransactionAsyncResult> withResult)
        {
            return Query(query).Then(withResult);
        }

        public RelClientBuilder QueryFrom(string filename, Action<TransactionAsyncResult> withResult)
        {
            string curDir = Directory.GetCurrentDirectory();
            var path = Path.Combine(Path.GetDirectoryName(Assembly.GetExecutingAssembly().Location), @"Data\Names.txt");
            var lines = File.ReadAllLines(path);
            var sb = new StringBuilder();
            foreach (var line in lines)
            {
                sb.AppendLine(line);
            }

            return Query(sb.ToString(), withResult);
        }

        public RelClientBuilder Then(Action<TransactionAsyncResult> withResult)
        {
            withResult(_result);
            return this;
        }

        public RelClientBuilder WithModels(Action<string> toOutput)
        {
            return Query(RelQueryBuilder
                .FromResource(Assembly.GetExecutingAssembly(), "GetModelNames.rel"))
                .Then(result =>
                {
                    
                });
        }
    }
}