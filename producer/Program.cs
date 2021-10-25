using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using producer.Configs;
using producer.Tools;
using System;
using System.IO;
using System.Threading.Tasks;

namespace producer
{
    class Program
    {
        private static ServiceProvider _ServiceProvider;

        static async Task Main()
        {
            var _settingsPath = Path.Combine(Environment.CurrentDirectory, "appsettings.json");

            Console.WriteLine(_settingsPath);
            
            IConfiguration _configuration = new ConfigurationBuilder().
                AddJsonFile(_settingsPath, true, true).
                Build();

            IServiceCollection _services = new ServiceCollection();

            _services.AddSingleton(_configuration);

            _services.Configure<KafkaConfig>(_configuration.GetSection("Kafka"));

            _services.AddSingleton<KafkaTool>();

            _ServiceProvider = _services.BuildServiceProvider();

            var _kafkaTool = _ServiceProvider.GetService<KafkaTool>();

            while (true)
            {
                Console.Write("message: ");

                var _message = Console.ReadLine();

                await _kafkaTool.ProduceAsync(_kafkaTool.Config.RTopic, _message);
            }
        }
    }
}
