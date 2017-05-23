using System;
using System.Collections.Generic;
using System.Net.Sockets;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Azure.EventHubs;
using Polly;

namespace EventHubTest
{
    public class LineDownloader
    {
        private readonly string _connectionString;
        private readonly string _partitionName;
        private readonly string _consumerGroup;
        private EventHubClient _client;
        private PartitionReceiver _receiver;
        private readonly Policy _retryPolicy;
        private readonly long _epoch = DateTime.UtcNow.Ticks;
        private string _latestOffset;

        public LineDownloader(string host, string keyName, string keyValue, string eventHubName, string partitionName, string consumerGroup, string latestOffset)
        {
            var builder = new EventHubsConnectionStringBuilder(new Uri($"amqps://{host}"), eventHubName, keyName, keyValue);
            _connectionString = builder.ToString();
            _partitionName = partitionName;
            _consumerGroup = consumerGroup;
            _latestOffset = latestOffset;
            _retryPolicy = Policy
                .Handle<Exception>(ex =>
                {
                    switch (ex)
                    {
                        case EventHubsException _: return true;
                        case UnauthorizedAccessException _: return true;
                        case TimeoutException _: return true;
                        case SocketException _: return true;
                        default:
                            Console.WriteLine($"Caught {ex.Message} ({ex.GetType()}), but not handling");
                            return false;
                    }
                })
                .WaitAndRetryForeverAsync(_ => TimeSpan.FromSeconds(10),
                (ex, ts) =>
                {
                    Console.WriteLine($"Caught {ex.Message}, delaying for {ts}");
                    return CloseClientIfRequiredAsync();
                });
        }

        private async Task CloseClientIfRequiredAsync()
        {
            try
            {
                if (_receiver != null)
                {
                    await _receiver.CloseAsync().ConfigureAwait(false);
                    _receiver = null;
                    await _client.CloseAsync().ConfigureAwait(false);
                    _client = null;
                    Console.WriteLine("Receiver and client closed");
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Error closing: {ex.Message}");
                throw;
            }
        }

        private void CloseClientIfRequired()
        {
            if (_receiver != null)
            {
                _receiver.Close();
                _receiver = null;
                _client.Close();
                _client = null;
            }
        }

        private PartitionReceiver GetOrCreateReceiver()
        {
            var receiver = _receiver;
            if (receiver == null)
            {
                var client = EventHubClient.CreateFromConnectionString(_connectionString);
                client.RetryPolicy = RetryPolicy.NoRetry;
                _client = client;

                receiver = client.CreateEpochReceiver(_consumerGroup, _partitionName, _latestOffset ?? PartitionReceiver.EndOfStream, _latestOffset == null, _epoch);
                receiver.RetryPolicy = RetryPolicy.NoRetry;
                _receiver = receiver;
            }
            return receiver;
        }

        public async Task<(string latestOffset, int messagesReceived)> TryDownloadLinesAsync(TimeSpan duration, CancellationToken ct)
        {
            using (ct.Register(() => CloseClientIfRequired()))
            {
                var events = (await ReceiveAsync(duration, ct).ConfigureAwait(false)) as IList<EventData>;
                var messagesReceived = events?.Count ?? 0;
                if (messagesReceived > 0)
                {
                    var latestOffset = events[messagesReceived - 1].SystemProperties.Offset;
                    _latestOffset = latestOffset;
                    return (latestOffset, messagesReceived);
                }
                else
                {
                    return (null, 0);
                }
            }
        }

        private Task<IEnumerable<EventData>> ReceiveAsync(TimeSpan duration, CancellationToken ct) =>
            _retryPolicy.ExecuteAsync(_ => GetOrCreateReceiver().ReceiveAsync(100, duration), ct, false);
    }
}
