using System;
using System.CommandLine;
using System.CommandLine.NamingConventionBinder;
using System.Threading.Tasks;
using RelationalAI.Fluent;

namespace RelationalAI.Examples
{
    public class ListModels
    {
        public static Command GetCommand()
        {
            var cmd = new Command("ListModels", "--state <State> --profile <Profile name>"){
                new Option<string>("--database"){
                    IsRequired = false,
                    Description = "To list models in a paricular database."
                },

                new Option<string>("--engine"){
                    IsRequired = false,
                    Description = "Engine to use to list models."
                },

                new Option<string>("--profile"){
                    IsRequired = false,
                    Description = "Profile name from .rai/config to connect to RAI Cloud."
                }
            };
            cmd.Description = "List databases";
            cmd.Handler = CommandHandler.Create<string, string, string>(Run);
            return cmd;
        }

        private static async Task Run(string database, string engine, string profile = "default")
        {
            RelClientBuilder
                .WithConnection(profile)
                .WithEngine(engine)
                .WithDatabase(database)
                .Query(RelQueryBuilder.GetModels())
                .Then(r => r.Results.ToArrayList().Print(Console.WriteLine));
        }

    }
}