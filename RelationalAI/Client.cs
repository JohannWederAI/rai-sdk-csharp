/*
 * Copyright 2022 RelationalAI, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

using System;
using System.Collections.Generic;
using System.Linq;
using System.Net.Http;
using System.Text;
using System.Threading.Tasks;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using Polly;
using Relationalai.Protocol;

namespace RelationalAI
{
    public class Client
    {
        private const string PathEngine = "/compute";
        private const string PathDatabase = "/database";
        private const string PathTransaction = "/transaction";
        private const string PathTransactions = "/transactions";
        private const string PathUsers = "/users";
        private const string PathOAuthClients = "/oauth-clients";
        private readonly Rest _rest;
        private readonly Context _context;

        public Client(Context context)
        {
            _context = context;
            _rest = new Rest(context);
        }

        public HttpClient HttpClient
        {
            get { return _rest.HttpClient; }
            set { _rest.HttpClient = value; }
        }

        public Task<Database> CreateDatabaseAsync(string database, string engine)
        {
            return CreateDatabaseAsync(database, engine, false);
        }

        public async Task<Database> CreateDatabaseAsync(string database, string engine, bool overwrite)
        {
            var mode = CreateMode(null, overwrite);
            var transaction = new Transaction(_context.Region, database, engine, mode);
            await _rest.PostAsync(MakeUrl(PathTransaction), transaction.Payload(null), null, transaction.QueryParams());
            return await GetDatabaseAsync(database);
        }

        public async Task<Database> CloneDatabaseAsync(
            string database,
            string engine,
            string source,
            bool overwrite = false)
        {
            var mode = CreateMode(source, overwrite);
            var tx = new Transaction(_context.Region, database, engine, mode, false, source);
            await _rest.PostAsync(MakeUrl(PathTransaction), tx.Payload(null), null, tx.QueryParams());
            return await GetDatabaseAsync(database);
        }

        public async Task<Database> GetDatabaseAsync(string database)
        {
            var parameters = new Dictionary<string, string>
            {
                { "name", database }
            };

            var resp = await GetResourceAsync(PathDatabase, null, parameters);
            var dbs = Json<GetDatabaseResponse>.Deserialize(resp).Databases;
            return dbs.Count > 0 ? dbs[0] : throw new HttpError(404, $"Database with name `{database}` not found");
        }

        public async Task<List<Database>> ListDatabasesAsync(DatabaseState? state = null)
        {
            var parameters = new Dictionary<string, string>();
            if (state != null)
            {
                parameters.Add("state", state.Value.Value());
            }

            var resp = await ListCollectionsAsync(PathDatabase, null, parameters);
            return Json<ListDatabasesResponse>.Deserialize(resp).Databases;
        }

        public async Task<DeleteDatabaseResponse> DeleteDatabaseAsync(string database)
        {
            var data = new Dictionary<string, string>
            {
                { "name", database }
            };
            var resp = await _rest.DeleteAsync(MakeUrl(PathDatabase), data) as string;
            return Json<DeleteDatabaseResponse>.Deserialize(resp);
        }

        public async Task<Engine> CreateEngineAsync(string engine, string size = "XS")
        {
            var data = new Dictionary<string, string>
            {
                { "region", _context.Region },
                { "name", engine },
                { "size", size.ToString() }
            };
            var resp = await _rest.PutAsync(MakeUrl(PathEngine), data) as string;
            return Json<CreateEngineResponse>.Deserialize(resp).Engine;
        }

        [Obsolete("This method is deprecated, please use the exposed http client instead")]
        public async Task<Engine> CreateEngineWithVersionAsync(string engine, string version, string size = "XS")
        {
            var data = new Dictionary<string, string>
            {
                { "region", _context.Region },
                { "name", engine },
                { "size", size.ToString() }
            };
            var headers = new Dictionary<string, string>
            {
                { "x-rai-parameter-compute-version", version },
            };

            var resp = await _rest.PutAsync(MakeUrl(PathEngine), data, headers: headers) as string;
            return Json<CreateEngineResponse>.Deserialize(resp).Engine;
        }

        public async Task<Engine> CreateEngineWaitAsync(string engine, string size = "XS")
        {
            var startTime = DateTimeOffset.Now.ToUnixTimeMilliseconds();
            await CreateEngineAsync(engine, size);
            var resp = await Policy
                    .HandleResult<Engine>(e => !EngineStates.IsTerminalState(e.State, EngineStates.Provisioned))
                    .RetryWithTimeout(startTime, 0.1, 120, 10 * 60)
                    .ExecuteAsync(() => GetEngineAsync(engine));

            if (resp.State != EngineStates.Provisioned)
            {
                throw new EngineProvisionFailedException(resp);
            }

            return resp;
        }

        public async Task<Engine> GetEngineAsync(string engine)
        {
            var parameters = new Dictionary<string, string>
            {
                { "name", engine },
                { "deleted_on", string.Empty }
            };
            var resp = await GetResourceAsync(PathEngine, null, parameters);
            var engines = Json<GetEngineResponse>.Deserialize(resp).Engines;
            return engines.Count > 0 ? engines[0] : throw new HttpError(404, $"Engine with name `{engine}` not found");
        }

        public async Task<List<Engine>> ListEnginesAsync(string state = null)
        {
            var parameters = new Dictionary<string, string>();
            if (state != null)
            {
                parameters.Add("state", state);
            }

            var resp = await ListCollectionsAsync(PathEngine, null, parameters);
            return Json<ListEnginesResponse>.Deserialize(resp).Engines;
        }

        public async Task<DeleteEngineResponse> DeleteEngineAsync(string engine)
        {
            var data = new Dictionary<string, string>
            {
                { "name", engine }
            };
            var resp = await _rest.DeleteAsync(MakeUrl(PathEngine), data) as string;
            return Json<DeleteEngineResponse>.Deserialize(resp);
        }

        public async Task<DeleteEngineResponse> DeleteEngineWaitAsync(string engine)
        {
            var startTime = DateTimeOffset.Now.ToUnixTimeMilliseconds();
            var resp = await DeleteEngineAsync(engine);
            var engineResponse = await Policy
                .HandleResult<Engine>(e => !EngineStates.IsFinalState(e.State))
                .RetryWithTimeout(startTime, 0.1, 120, 10 * 60)
                .ExecuteAsync(() => GetEngineAsync(engine));
            resp.Status.State = engineResponse.State;
            return resp;
        }

        public async Task<OAuthClient> CreateOAuthClientAsync(string name, List<Permission> permissions = null)
        {
            var uniquePermissions = new HashSet<string>();
            permissions?.ForEach(p => uniquePermissions.Add(p.Value()));
            var data = new Dictionary<string, object>
            {
                { "name", name },
                { "permissions", uniquePermissions }
            };
            var resp = await _rest.PostAsync(MakeUrl(PathOAuthClients), data) as string;
            return Json<CreateOAuthClientResponse>.Deserialize(resp).OAuthClient;
        }

        public async Task<OAuthClient> FindOAuthClientAsync(string name)
        {
            var clients = await ListOAuthClientsAsync();

            return clients.FirstOrDefault(client => client.Name == name) ??
                   throw new HttpError(404, $"OAuth Client with name `{name}` not found");
        }

        public async Task<OAuthClientEx> GetOAuthClientAsync(string id)
        {
            var resp = await GetResourceAsync($"{PathOAuthClients}/{id}");
            return Json<GetOAuthClientResponse>.Deserialize(resp).Client;
        }

        public async Task<List<OAuthClient>> ListOAuthClientsAsync()
        {
            var resp = await ListCollectionsAsync(PathOAuthClients);
            return Json<ListOAuthClientResponse>.Deserialize(resp).Clients;
        }

        public async Task<DeleteOAuthClientResponse> DeleteOAuthClientAsync(string id)
        {
            var resp = await _rest.DeleteAsync(MakeUrl($"{PathOAuthClients}/{id}")) as string;
            return Json<DeleteOAuthClientResponse>.Deserialize(resp);
        }

        public async Task<User> CreateUserAsync(string email, List<Role> roles = null)
        {
            var uniqueRoles = new HashSet<string>();
            roles?.ForEach(r => uniqueRoles.Add(r.Value()));
            var data = new Dictionary<string, object>
            {
                { "email", email },
                { "roles", uniqueRoles }
            };
            var resp = await _rest.PostAsync(MakeUrl(PathUsers), data) as string;
            return Json<CreateUserResponse>.Deserialize(resp).User;
        }

        public async Task<User> UpdateUserAsync(string id, UserStatus status = UserStatus.None, List<Role> roles = null)
        {
            var data = new Dictionary<string, object>();
            if (roles != null)
            {
                var uniqueRoles = new HashSet<string>();
                roles.ForEach(r => uniqueRoles.Add(r.Value()));
                data.Add("roles", uniqueRoles);
            }

            if (status != UserStatus.None)
            {
                data.Add("status", status.Value());
            }

            var resp = await _rest.PatchAsync(MakeUrl($"{PathUsers}/{id}"), data) as string;
            return Json<UpdateUserResponse>.Deserialize(resp).User;
        }

        public async Task<User> FindUserAsync(string email)
        {
            var users = await ListUsersAsync();

            return users.FirstOrDefault(user => user.Email == email) ??
                   throw new HttpError(404, $"User with email `{email}` not found");
        }

        public async Task<User> GetUserAsync(string userId)
        {
            var resp = await GetResourceAsync($"{PathUsers}/{userId}");
            return Json<GetUserResponse>.Deserialize(resp).User;
        }

        public async Task<List<User>> ListUsersAsync()
        {
            var resp = await ListCollectionsAsync(PathUsers);
            return Json<ListUsersResponse>.Deserialize(resp).Users;
        }

        public async Task<DeleteUserResponse> DeleteUserAsync(string id)
        {
            var resp = await _rest.DeleteAsync(MakeUrl($"{PathUsers}/{id}")) as string;
            return Json<DeleteUserResponse>.Deserialize(resp);
        }

        public Task<User> DisableUserAsync(string id)
        {
            return UpdateUserAsync(id, UserStatus.InActive);
        }

        public Task<User> EnableUserAsync(string id)
        {
            return UpdateUserAsync(id, UserStatus.Active);
        }

        public async Task<TransactionAsyncMultipleResponses> GetTransactionsAsync()
        {
            var rsp = await _rest.GetAsync(MakeUrl(PathTransactions)) as string;
            return Json<TransactionAsyncMultipleResponses>.Deserialize(rsp);
        }

        public async Task<TransactionAsyncSingleResponse> GetTransactionAsync(string id)
        {
            var rsp = await _rest.GetAsync(MakeUrl($"{PathTransactions}/{id}")) as string;
            return Json<TransactionAsyncSingleResponse>.Deserialize(rsp);
        }

        public async Task<List<ArrowRelation>> GetTransactionResultsAsync(string id)
        {
            var files = await _rest.GetAsync(MakeUrl($"{PathTransactions}/{id}/results")) as List<TransactionAsyncFile>;
            return _rest.ReadArrowFiles(files);
        }

        public async Task<MetadataInfo> GetTransactionMetadataAsync(string id)
        {
            var headers = new Dictionary<string, string>
            {
                { "accept", "application/x-protobuf" },
            };

            return await _rest.GetAsync(MakeUrl($"{PathTransactions}/{id}/metadata"), headers: headers) as MetadataInfo;
        }

        public async Task<List<object>> GetTransactionProblemsAsync(string id)
        {
            var rsp = await _rest.GetAsync(MakeUrl($"{PathTransactions}/{id}/problems")) as string;
            return ParseProblemsResult(rsp);
        }

        public async Task<TransactionAsyncCancelResponse> CancelTransactionAsync(string id)
        {
            var rsp = await _rest.PostAsync(MakeUrl($"{PathTransactions}/{id}/cancel"), new Dictionary<string, object>()) as string;
            return Json<TransactionAsyncCancelResponse>.Deserialize(rsp);
        }

        public async Task<string> DeleteTransactionAsync(string id)
        {
            var resp = await _rest.DeleteAsync(MakeUrl($"{PathTransactions}/{id}")) as string;
            return FormatResponse(resp);
        }

        public async Task<List<Edb>> ListEdbsAsync(string database, string engine)
        {
            var tx = new Transaction(_context.Region, database, engine, TransactionMode.Open);
            var actions = new List<DbAction> { DbAction.MakeListEdbAction() };
            var body = tx.Payload(actions);
            var resp = await _rest.PostAsync(MakeUrl(PathTransaction), body, null, tx.QueryParams()) as string;
            var actionsResp = Json<ListEdbsResponse>.Deserialize(resp).Actions;
            return actionsResp.Count == 0 ? new List<Edb>() : actionsResp[0].Result.Rels;
        }

        public async Task<TransactionAsyncResult> LoadModelsAsync(
            string database,
            string engine,
            Dictionary<string, string> models)
        {
            var queries = new List<string>();
            var queriesInputs = new Dictionary<string, string>();
            var randInt = new Random().Next(int.MaxValue);

            var index = 0;
            foreach (var model in models)
            {
                var inputName = $"input_{randInt}_{index}";
                queries.Add($"def delete:rel:catalog:model[\"{model.Key}\"] = rel:catalog:model[\"{model.Key}\"] \n" +
                $"def insert:rel:catalog:model[\"{model.Key}\"] = {inputName}");
                queriesInputs.Add(inputName, model.Value);

                index++;
            }

            return await ExecuteAsync(database, engine, string.Join('\n', queries), false, queriesInputs);
        }

        public async Task<TransactionAsyncResult> LoadModelsWaitAsync(
            string database,
            string engine,
            Dictionary<string, string> models)
        {
            var queries = new List<string>();
            var queriesInputs = new Dictionary<string, string>();
            var randInt = new Random().Next(int.MaxValue);

            var index = 0;
            foreach (var model in models)
            {
                var inputName = $"input_{randInt}_{index}";
                queries.Add($"def delete:rel:catalog:model[\"{model.Key}\"] = rel:catalog:model[\"{model.Key}\"] \n" +
                $"def insert:rel:catalog:model[\"{model.Key}\"] = {inputName}");
                queriesInputs.Add(inputName, model.Value);

                index++;
            }

            return await ExecuteWaitAsync(database, engine, string.Join('\n', queries), false, queriesInputs);
        }

        public async Task<TransactionAsyncResult> DeleteModelsAsync(string database, string engine, List<string> models)
        {
            var queries = new List<string>();
            foreach (var model in models)
            {
                queries.Add($"def delete:rel:catalog:model[\"{model}\"] = rel:catalog:model[\"{model}\"]");
            }

            return await ExecuteWaitAsync(database, engine, string.Join('\n', queries), false);
        }

        // Query
        public async Task<TransactionResult> ExecuteV1Async(
            string database,
            string engine,
            string source,
            bool readOnly = false,
            Dictionary<string, string> inputs = null)
        {
            var tx = new Transaction(_context.Region, database, engine, TransactionMode.Open, readOnly);
            var actions = new List<DbAction> { DbAction.MakeQueryAction(source, inputs) };
            var body = tx.Payload(actions);
            var resp = await _rest.PostAsync(MakeUrl(PathTransaction), body, null, tx.QueryParams()) as string;
            return Json<TransactionResult>.Deserialize(resp);
        }

        public async Task<TransactionAsyncResult> ExecuteWaitAsync(
            string database,
            string engine,
            string source,
            bool readOnly = false,
            Dictionary<string, string> inputs = null)
        {
            var startTime = DateTimeOffset.Now.ToUnixTimeMilliseconds();
            var rsp = await ExecuteAsync(database, engine, source, readOnly, inputs);
            var id = rsp.Transaction.Id;

            // fast-path
            if (rsp.GotCompleteResult)
            {
                return rsp;
            }

            // slow-path
            var transactionResponse = await Policy
                .HandleResult<TransactionAsyncSingleResponse>(r => !r.Transaction.State.IsFinalState())
                .RetryForeverWithBoundedDelay(startTime, 0.2) // wait for 20% of the total runtime
                .ExecuteAsync(() => GetTransactionAsync(id));

            var transaction = transactionResponse.Transaction;
            List<ArrowRelation> results = null;
            MetadataInfo metadata = null;
            List<object> problems = null;

            if (transaction.State == TransactionAsyncState.Completed || TransactionAsyncAbortReason.IntegrityConstraintViolation.Equals(transaction.AbortReason))
            {
                results = await GetTransactionResultsAsync(id);
                metadata = await GetTransactionMetadataAsync(id);
                problems = await GetTransactionProblemsAsync(id);
            }

            return new TransactionAsyncResult(
                transaction,
                results,
                metadata,
                problems,
                true);
        }

        public async Task<TransactionAsyncResult> ExecuteAsync(
            string database,
            string engine,
            string source,
            bool readOnly = false,
            Dictionary<string, string> inputs = null)
        {
            var tx = new TransactionAsync(database, engine, readOnly, source, inputs);
            var body = tx.Payload();
            var rsp = await _rest.PostAsync(MakeUrl(PathTransactions), body, null, tx.QueryParams());

            if (rsp is string s)
            {
                var txn = Json<TransactionAsyncCompactResponse>.Deserialize(s);
                return new TransactionAsyncResult(txn, new List<ArrowRelation>(), null, new List<object>());
            }

            return ReadTransactionAsyncResults(rsp as List<TransactionAsyncFile>);
        }

        public Task<TransactionResult> LoadJsonAsync(
            string database,
            string engine,
            string relation,
            string data)
        {
            var inputs = new Dictionary<string, string>
            {
                { "data", data }
            };
            var source = GenLoadJson(relation);
            return ExecuteV1Async(database, engine, source, false, inputs);
        }

        public Task<TransactionResult> LoadCsvAsync(
            string database,
            string engine,
            string relation,
            string data,
            CsvOptions options = null)
        {
            var source = GenLoadCsv(relation, options);
            var inputs = new Dictionary<string, string>
            {
                { "data", data }
            };
            return ExecuteV1Async(database, engine, source, false, inputs);
        }

        private static TransactionMode CreateMode(string source, bool overwrite)
        {
            if (source != null)
            {
                return overwrite ? TransactionMode.CloneOverwrite : TransactionMode.Clone;
            }

            return overwrite ? TransactionMode.CreateOverwrite : TransactionMode.Create;
        }

        private static string GenLoadJson(string relation)
        {
            var builder = new StringBuilder();
            builder.Append("\ndef config:data = data\n");
            builder.AppendFormat("def insert:{0} = load_json[config]\n", relation);
            return builder.ToString();
        }

        private static void GenSchemaConfig(StringBuilder builder, CsvOptions options)
        {
            var schema = options?.Schema;

            if (schema == null || schema.Count == 0)
            {
                return;
            }

            var config = schema.Aggregate(
                "def config:schema = {",
                (current, entry) => current + $"\n    :{entry.Key}, \"{entry.Value}\";");

            builder.Append(config);
            builder.Append("\n}\n");
        }

        private static string GenLiteral(Int64 value)
        {
            return value.ToString();
        }

        private static string GenLiteral(char value)
        {
            return value == '\'' ? "'\\''" : $"'{value}'";
        }

        private static string GenLiteral(object value)
        {
            switch (value)
            {
                case null:
                    throw new ArgumentException("Cannot generate literal from null value");
                case Int16 _:
                case Int32 _:
                case Int64 _:
                    return GenLiteral(Convert.ToInt64(value));
                case char c:
                    return GenLiteral(c);
                default:
                    throw new ArgumentException($"Cannot generate literal from {value.GetType()} value");
            }
        }

        private static void GenSyntaxOption(StringBuilder builder, string name, object value)
        {
            if (value == null)
            {
                return;
            }

            var lit = GenLiteral(value);
            var def = $"def config:syntax:{name} = {lit}\n";
            builder.Append(def);
        }

        private static void GenSyntaxConfig(StringBuilder builder, CsvOptions options)
        {
            if (options == null)
            {
                return;
            }

            GenSyntaxOption(builder, "header_row", options.HeaderRow);
            GenSyntaxOption(builder, "delim", options.Delim);
            GenSyntaxOption(builder, "escapechar", options.EscapeChar);
            GenSyntaxOption(builder, "quotechar", options.QuoteChar);
        }

        private static string GenLoadCsv(string relation, CsvOptions options)
        {
            var builder = new StringBuilder();
            GenSchemaConfig(builder, options);
            GenSyntaxConfig(builder, options);
            builder.Append("\n def config:data = data\n");
            builder.AppendFormat("def insert:{0} = load_csv[config]\n", relation);

            return builder.ToString();
        }

        private static string FormatResponse(string response, string key = null)
        {
            try
            {
                // to return the formatted JSON
                var json = JObject.Parse(response);
                JToken result = json;
                if (key != null && json.ContainsKey(key))
                {
                    result = json.GetValue(key);
                }

                return result?.ToString() ?? response;
            }
            catch
            {
                // ignore exception and return initial response as is
                return response;
            }
        }

        private static JToken GetValueFromResponse(string response, string valueKey)
        {
            var json = JObject.Parse(response);
            JToken result = json;
            if (valueKey != null && json.ContainsKey(valueKey))
            {
                result = json.GetValue(valueKey);
            }

            return result;
        }

        private static List<object> ParseProblemsResult(string rsp)
        {
            var output = new List<object>();

            var problems = JsonConvert.DeserializeObject(rsp);
            if (!(problems is JArray problemsArray))
            {
                throw new InvalidResponseException("Unexpected format of transaction problems", rsp);
            }

            foreach (var problem in problemsArray)
            {
                var data = JsonConvert.SerializeObject(problem);
                try
                {
                    output.Add(Json<IntegrityConstraintViolation>.Deserialize(data));
                }
                catch (InvalidResponseException)
                {
                    output.Add(Json<ClientProblem>.Deserialize(data));
                }
            }

            return output;
        }

        private TransactionAsyncResult ReadTransactionAsyncResults(List<TransactionAsyncFile> files)
        {
            var transaction = files.Find(f => f.Name == "transaction");
            var metadata = files.Find(f => f.Name == "metadata.proto");
            var problems = files.Find(f => f.Name == "problems");

            if (transaction == null)
            {
                throw new InvalidResponseException("Transaction part of async result not found");
            }

            if (metadata == null)
            {
                throw new InvalidResponseException("Metadata part of async result not found");
            }

            var transactionResult = Json<TransactionAsyncCompactResponse>.Deserialize(_rest.ReadString(transaction.Data));
            var metadataProto = _rest.ReadMetadataProtobuf(metadata.Data);

            List<object> problemsResult = null;
            if (problems != null)
            {
                problemsResult = ParseProblemsResult(_rest.ReadString(problems.Data));
            }

            var results = _rest.ReadArrowFiles(files);

            return new TransactionAsyncResult(transactionResult, results, metadataProto, problemsResult, true);
        }

        private async Task<string> GetResourceAsync(string path, string key = null, Dictionary<string, string> parameters = null)
        {
            var response = await GetStringResponseAsync(path, parameters);

            try
            {
                var result = GetValueFromResponse(response, key);
                if (result is { Type: JTokenType.Array, HasValues: true })
                {
                    // making sure there aren't more than one value
                    if (result.First != result.Last)
                    {
                        throw new InvalidResponseException("More than one resource found", response);
                    }

                    result = result.First;
                }

                return result?.ToString() ?? response;
            }
            catch
            {
                // ignore exception and return raw response
                return response;
            }
        }

        private async Task<string> ListCollectionsAsync(string path, string key = null, Dictionary<string, string> parameters = null)
        {
            var response = await GetStringResponseAsync(path, parameters);

            try
            {
                var result = GetValueFromResponse(response, key);
                return result?.ToString() ?? response;
            }
            catch
            {
                // ignore exception and return raw response
                return response;
            }
        }

        private async Task<string> GetStringResponseAsync(string path, Dictionary<string, string> parameters = null)
        {
            var url = MakeUrl(path);
            var response = await _rest.GetAsync(url, null, null, parameters);
            if (!(response is string stringResponse))
            {
                throw new InvalidResponseException(
                    $"Unexpected response type, expected a string but received {response.GetType().Name}",
                    response);
            }

            return stringResponse;
        }

        private string MakeUrl(string path)
        {
            return $"{_context.Scheme}://{_context.Host}:{_context.Port}{path}";
        }

        public class Context : Rest.Context
        {
            private string _host;
            private string _port;
            private string _scheme;

            public Context(
                string host = null,
                string port = null,
                string scheme = null,
                string region = null,
                ICredentials credentials = null)
            {
                Host = host;
                Port = port;
                Scheme = scheme;
                Region = region;
                Credentials = credentials;
            }

            public Context(Dictionary<string, object> config)
            {
                if (config == null)
                {
                    return;
                }

                Host = (config.ContainsKey("host") && config["host"] != null) ? (string)config["host"] : null;
                Port = (config.ContainsKey("port") && config["port"] != null) ? (string)config["port"] : null;
                Scheme = (config.ContainsKey("scheme") && config["scheme"] != null) ? (string)config["scheme"] : null;
                Region = (config.ContainsKey("region") && config["region"] != null) ? (string)config["region"] : null;
                Credentials = (config.ContainsKey("credentials") && config["credentials"] != null) ?
                    (ICredentials)config["credentials"] : null;
            }

            public string Host
            {
                get => _host;
                set => _host = !string.IsNullOrEmpty(value) ? value : "localhost";
            }

            public string Port
            {
                get => _port;
                set => _port = !string.IsNullOrEmpty(value) ? value : "443";
            }

            public string Scheme
            {
                get => _scheme;
                set => _scheme = !string.IsNullOrEmpty(value) ? value : "https";
            }
        }
    }
}