using System;
using System.Collections.Generic;
using System.CommandLine;
using System.CommandLine.NamingConventionBinder;

using RAILib;


namespace RAILib.Examples
{
    public class DeleteOAuthClient
    {   
        public static Command GetCommand()
        {
            var cmd = new Command("DeleteOAuthClient", "--id <Client ID> --profile <Profile name>"){
                new Option<string>("--id"){
                    IsRequired = true,
                    Description = "oAuth client's id to delete."
                },

                new Option<string>("--profile"){
                    IsRequired = false,
                    Description = "Profile name from .rai/config to connect to RAI Cloud."
                }
            };
            cmd.Description = "Deletes an oAuth client by id.";
            cmd.Handler = CommandHandler.Create<string, string>(Run);
            return cmd;
        }

        private static void Run(string id, string profile = "default")
        {
            Dictionary<string, object> config = Config.Read("", profile);
            Api.Context context = new Api.Context(config);
            Api api = new Api(context);
            Console.WriteLine(api.DeleteOAuthClient(id));
        }

    }
}