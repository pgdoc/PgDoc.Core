using Microsoft.Extensions.Configuration;

namespace PgDoc.Tests
{
    public class ConfigurationManager
    {
        public static string GetSetting(string key)
        {
            IConfigurationRoot configuration = new ConfigurationBuilder().AddJsonFile("config.json").Build();

            return configuration[key];
        }
    }
}
