using System;
using System.Collections.Generic;
using System.CommandLine;
using System.CommandLine.NamingConventionBinder;

using RAILib;


namespace RAILib.Examples
{
    public class GetUser
    {
        public static Command GetCommand()
        {
            var cmd = new Command("GetUser", "--id <User ID> --profile <Profile name>"){
                new Option<string>("--id"){
                    IsRequired = true,
                    Description = "User's ID to get the details."
                },

                new Option<string>("--profile"){
                    IsRequired = false,
                    Description = "Profile name from .rai/config to connect to RAI Cloud."
                }
            };
            cmd.Description = "Gets a user's details by ID.";
            cmd.Handler = CommandHandler.Create<string, string>(Run);
            
            return cmd;
        }

        private static void Run(string id, string profile = "default")
        {
            Dictionary<string, object> config = Config.Read("", profile);
            Api.Context context = new Api.Context(config);
            Api api = new Api(context);
            Console.WriteLine(api.GetUser(id));
        }

    }
}