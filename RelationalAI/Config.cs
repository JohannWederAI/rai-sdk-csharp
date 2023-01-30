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
using System.IO;
using System.Linq;
using System.Runtime.InteropServices;
using IniParser;
using IniParser.Model;

namespace RelationalAI
{
    public class Config
    {
        public static Dictionary<string, object> Read(string path = null, string profile = "default")
        {
            var data = LoadIniConfig(path);

            return ReadConfigFromInitData(data, profile);
        }

        public static Dictionary<string, object> Read(MemoryStream stream, string profile = "default")
        {
            var parser = new FileIniDataParser();
            var data = parser.ReadData(new StreamReader(stream));

            return ReadConfigFromInitData(data, profile);
        }

        public static string GetRaiConfigPath()
        {
            return Path.Combine(GetRaiDir(), "config");
        }

        private static Dictionary<string, object> ReadConfigFromInitData(IniData data, string profile)
        {
            string[] keys = { "host", "port", "scheme", "region", "engine", "database" };
            var config = keys
                .ToDictionary<string, string, object>(key => key, key => GetIniValue(data, profile, key, null));

            var clientCredentials = ReadClientCredentials(data, profile);
            if (clientCredentials != null)
            {
                config.Add("credentials", clientCredentials);
            }

            return config;
        }

        private static ICredentials ReadClientCredentials(IniData data, string profile)
        {
            var clientId = GetIniValue(data, profile, "client_id", null);
            var clientSecret = GetIniValue(data, profile, "client_secret", null);
            var clientCredentialsUrl = GetIniValue(data, profile, "client_credentials_url", null);
            if (clientId != null && clientSecret != null)
            {
                return new ClientCredentials(clientId, clientSecret, clientCredentialsUrl);
            }

            return null;
        }

        private static string GetRaiDir()
        {
            var envHome = RuntimeInformation.IsOSPlatform(OSPlatform.Windows) ? "HOMEPATH" : "HOME";
            var home = Environment.GetEnvironmentVariable(envHome);
            if (RuntimeInformation.IsOSPlatform(OSPlatform.Windows))
            {
                var homeDrive = Environment.GetEnvironmentVariable("HOMEDRIVE");
                home = homeDrive + home;
            }

            return Path.Combine(home, ".rai");
        }

        private static IniData LoadIniConfig(string path)
        {
            var parser = new FileIniDataParser();

            path = !string.IsNullOrEmpty(path) ? path : GetRaiConfigPath();
            if (!File.Exists(path))
            {
                throw new FileNotFoundException(path + " does not exist");
            }

            return parser.ReadFile(path);
        }

        private static string GetIniValue(IniData ini, string profile, string key, string defaultValue = "notfound")
        {
            var keyData = ini[profile].GetKeyData(key);
            return keyData == null ? defaultValue : keyData.Value;
        }
    }
}
