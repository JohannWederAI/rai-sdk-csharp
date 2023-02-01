# Changelog
# v0.9.11-alpha
* Fix common retry policy to not retry on `HttpError` with 4xx Status Codes, but only on 5xx.
# v0.9.10-alpha
* Bump up google protobuf version.
# v0.9.9-alpha
* Fix `GetOAuthClientResponse` deserialization issue
# v0.9.8-alpha
* Added `TransactionAsyncAbortReason`
# v0.9.7-alpha
* Fixed load models async issue.

## v0.9.6-alpha
* Fixed transaction async backoff issue.

## v0.9.5-alpha
* Exposed http client
```
var client = new Client(ctx);
var httpClient = client.HttpClient;
```
* Poll transaction asynchronous with x% overhead of the time the transaction has been running so far.
## v0.9.4-alpha
* Refactored sdk code back into a single package.

## v0.9.3-alpha
* Removed `EngineState` enumeration:
    - `ListEnginesAsync(state = "PROVISIONED")`
## v0.9.2-alpha
* Removed `EngineSize` enumeration:
  - `CreateEngineAsync(engineName, size = "XS")`
## v0.9.1-alpha
* Fix for missing Protos package.
* Fix for `ListEngines`.

## v0.8.0-alpha
* Added the following exceptions:
  - `NotFoundException` thrown from `GetEngineAsync(string engine)`, `GetDatabaseAsync(string database)`, etc. when requested resource (Engine/Database/Model/User/Client) doesn't exist or got deleted.
  - `ApiException` thrown when RAI API responds with 5xx status codes or contains unsupported content type.
  - `EngineProvisionFailedException` thrown from `CreateEngineWaitAsync` when requested engine failed to provision.
  - `CredentialsNotSupportedException` thrown from every method when credentials to access RAI API are not provided or provided credentials are unsupported (i.e. not for OAuth Client credentials method).
  - `InvalidResponseException` thrown when RAI API response has unexpected format or content type.

## v0.7.0-alpha
* Replaced String properties with Enums in the following models returned by corresponding API methods:
  - `Database`.`State` property is of `DatabaseState` type.
  - `Engine`.`State` and `DeleteEngineStatus`.`State` properties are of `EngineState` type, `Engine`.`Size` property is of `EngineSize` type.
  - `Transaction`.`Mode` property is of `TransactionMode` type.
  - `TransactionAsyncCompactResponse`.`State` property is of `TransactionAsyncState` type.
  - `User`.`Status` property is of `UserStatus` type and `User`.`Roles` property is of `List<Role>` type.
* Changed the following API methods to accept Enum parameters instead of Strings:
  - `ListDatabasesAsync` accepts optional `state` parameter of `DatabaseState?` type.
  - `ListEnginesAsync` accepts optional `state` parameter of `EngineState?` type.
* Made structural changes that include moving classes from `RelationalAI` namespace to the following namespaces:
  - `RelationalAI.Models.Database` namespace: `CreateDatabaseResponse`, `Database`, `DeleteDatabaseResponse`, `GetDatabaseResponse`, `ListDatabasesResponse`.
  - `RelationalAI.Models.Edb` namespace: `Edb`, `ListEdbsResponse`, `ListEdbsResponseAction`, `ListEdbsResponseResult`.
  - `RelationalAI.Models.Engine` namespace: `CreateEngineResponse`, `DeleteEngineResponse`, `DeleteEngineStatus`, `Engine`, `EngineSize`, `GetEngineResponse`, `ListEnginesResponse`.
  - `RelationalAI.Models.OAuthClient` namespace: `CreateOAuthClientResponse`, `DeleteOAuthClientResponse`, `GetOAuthClientResponse`, `ListOAuthClientResponse`, `OAuthClient`, `OAuthClientEx`, `Permission`.
  - `RelationalAI.Models.RelModel` namespace: `ListModelsResponse`, `ListModelsResponseAction`, `ListModelsResponseResult`, `Model`.
  - `RelationalAI.Models.Transaction` namespace: `ArrowRelation`, `ClientProblem`, `CsvOptions`, `DbAction`, `IntegrityConstraintViolation`, `Relation`, `RelKey`, `Source`, `Transaction`, `TransactionAsync`, `TransactionAsyncCancelResponse`, `TransactionAsyncCompactResponse`, `TransactionAsyncFile`, `TransactionAsyncMetadataResponse`, `TransactionAsyncMultipleResponses`, `TransactionAsyncResponse`, `TransactionAsyncResult`, `TransactionAsyncSingleResponse`, `TransactionResult`.
  - `RelationalAI.Models.User` namespace: `CreateUserResponse`, `DeleteUserResponse`, `GetUserResponse`, `ListUsersResponse`, `Role`, `UpdateUserResponse`, `User`, `UserStatus`.
  - `RelationalAI.Services` namespace: `Client`, `Rest`.
* Added more exceptions thrown in the cases of unexpected responses from RAI REST API, such as:
  - `SystemException` with message "Unexpected format of problems" thrown when SDK fails to parse transaction problems.

## v0.6.0-alpha
* Deprecated metadata json format.
* Removed `TransactionAsyncMetadataResponse` model.
* Added support to metadata protobuf format.
* `GetTransactionMetadata` returns protobuf metadata.

## v0.5.0-alpha
* In this update we ensure the SDK follows the standard async/await approach for the methods. The following methods have been renamed to include the "Async" postfix, made async and now return a `Task<T>` to the user:
  - `CreateDatabase`, `GetDatabase`, `ListDatabases`, `DeleteDatabase`, `CloneDatabase`.
  - `CreateEngine`, `CreateEngineWait`, `GetEngine`, `ListEngines`, `DeleteEngine`.
  - `CreateOAuthClient`, `FindOAuthClient`, `GetOAuthClient`, `ListOAuthClients`, `DeleteOAuthClient`.
  - `CreateUser`, `UpdateUser`, `FindUser`, `GetUser`, `ListUsers`, `DeleteUser`, `DisableUser`, `EnableUser`.
  - `GetTransactions`, `GetTransaction`, `GetTransactionResults`, `GetTransactionMetadata`, `GetTransactionProblems`, `CancelTransaction`, `DeleteTransaction`.
  - `ListEdbs`, `LoadModel`, `LoadModels`, `ListModels`, `ListModelNames`, `GetModel`, `DeleteModel`.
  - `LoadJson`, `LoadCsv`.
  - `ExecuteV1`.
  - `Execute` renamed to `ExecuteWaitAsync` to match the naming of the sync operations.

## v0.4.0-alpha
* Renamed:
  - `Execute` to `ExecuteV1`.
  - `ExecuteAsyncWait` to `Execute`.

## v0.3.0-alpha
* Added `CancelTransaction` feature.

## v0.2.2-alpha
* Added `FinishedAt` field to `TransactionAsyncResponse`.

## v0.2.1-alpha
* Fixed `executeAsync` inputs issue.

## v0.2.0-alpha
* Added v2 predefined results formats:

    - `GetTransactions` returns `TransactionsAsyncMultipleResponses`.
    - `GetTransaction` returns `TransactionAsyncSingleResponse`.
    - `GetTransactionResults` returns `List<ArrowRelation>`.
    - `GetTransactionMetadata` returns `List<TransactionAsyncMetadataResponse>`.
    - `GetTransactionProblems` return `List<ClientProblem|IntegrityConstraintViolation>`.
    - `ExecuteAsync` returns `TransactionAsyncResult`.

## v0.1.0-alpha
* Added support to the asynchronous protocol including:
    - `executeAsync`: runs an asynchronous request.
    - `executeAsyncWait`: runs an asynchronous request and wait of its completion.
    - `getTransaction`: gets information about transaction.
    - `getTransactions`: gets the list of transactions.
    - `getTransactionResults`: gets transaction execution results.
    - `getTransactionMetadata`: gets transaction metadata.
    - `getTransactionProblems`: gets transaction execution problems.
* Added more api functionalities:
    - `ListEdbs`: lists the current edbs.
    - `LoadModel`: loads a model.
    - `LoadModels`: loads a dictionary of models.
    - `ListModels`: lists the current models.
    - `ListModelNames`: lists the current model names.
    - `GetModel`: gets a specific model.
    - `DeleteModel`: deletes a specific model.
    - `LoadJson`: loads json data.
    - `LoadCsv`: loads csv data.
    - `CloneDatabase`: clone a database.
