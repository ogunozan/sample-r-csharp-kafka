using Confluent.Kafka;
using Confluent.Kafka.Admin;
using producer.Configs;
using Microsoft.Extensions.Options;
using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace producer.Tools
{
    public class KafkaTool
    {
        private readonly ProducerConfig _ProducerConfig;

        private readonly ConsumerConfig _ConsumerConfig;

        public KafkaConfig Config { get; }

        public KafkaTool(IOptions<KafkaConfig> _config)
        {
            Config = _config.Value;

            _ProducerConfig = new ProducerConfig
            {
                BootstrapServers = Config.Server,
            };

            _ConsumerConfig = new ConsumerConfig
            {
                BootstrapServers = Config.Server,
                GroupId = Config.GroupId,
                EnableAutoCommit = false,
                AllowAutoCreateTopics = false,
                AutoOffsetReset = AutoOffsetReset.Earliest
            };
        }

        public async Task ConsumeAsync<T>(string _topic, Action<T> _action)
        {
            await CreateTopicAsync(_topic);

            _ = Task.Run(() =>
            {
                using var _consumer = new ConsumerBuilder<Ignore, string>(_ConsumerConfig).
                    Build();

                _consumer.Subscribe(_topic);

                while (true)
                {
                    var _result = _consumer.Consume();

                    _action.Invoke(JsonConvert.DeserializeObject<T>(_result.Message.Value));

                    _consumer.Commit();
                }
            });
        }

        public async Task ProduceAsync<T>(string _topic, T _message)
        {
            try
            {
                using var _producer = new ProducerBuilder<Null, string>(_ProducerConfig).
                    Build();

                await _producer.ProduceAsync(_topic,
                    new Message<Null, string>
                    {
                        Value = JsonConvert.SerializeObject(_message)
                    });
            }
            catch (Exception _ex)
            {
            }
        }

        public async Task CreateTopicAsync(string _topic)
        {
            try
            {
                using var adminClient = new AdminClientBuilder(
                    new AdminClientConfig { BootstrapServers = Config.Server }).
                    Build();

                var _metadata = adminClient.GetMetadata(_topic, TimeSpan.FromSeconds(10));

                if (_metadata.Topics.Any())
                {
                    return;
                }

                await adminClient.CreateTopicsAsync(
                    new TopicSpecification[]
                    {
                        new TopicSpecification
                        {
                            Name = _topic,
                            ReplicationFactor = 1,
                            NumPartitions = 1
                        }
                    });
            }
            catch (CreateTopicsException _ex)
            {
                Console.WriteLine($"An error occured creating topic " +
                    $"{_ex.Results[0].Topic}: {_ex.Results[0].Error.Reason}");
            }
        }
    }
}