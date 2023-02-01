using System;
using System.CommandLine;
using System.CommandLine.NamingConventionBinder;
using System.Threading.Tasks;
using RelationalAI.Fluent;

namespace RelationalAI.Examples
{
    public class GetModel
    {
        public static Command GetCommand()
        {
            var cmd = new Command("GetModel", "--database <Database name> --engine <Engine name> --name <Model name> --profile <Profile name>"){
                new Option<string>("--database"){
                    IsRequired = true,
                    Description = "Database name to get model from"
                },

                new Option<string>("--engine"){
                    IsRequired = true,
                    Description = "Engine name to use."
                },

                new Option<string>("--name"){
                    IsRequired = true,
                    Description = "Model name to get"
                },

                new Option<string>("--profile"){
                    IsRequired = false,
                    Description = "Profile name from .rai/config to connect to RAI Cloud."
                }
            };
            cmd.Description = "Gets a model by name.";
            cmd.Handler = CommandHandler.Create<string, string, string, string>(Run);
            return cmd;
        }

        private static async Task Run(string database, string engine, string name, string profile = "default")
        {
            RelClientBuilder
                .WithConnection(profile)
                .WithEngine(engine)
                .WithDatabase(database)
                .Query(RelQueryBuilder.GetModel(name))
                .Then(r => r.Results.ToArrayList().Print(Console.WriteLine));
        }

    }
}