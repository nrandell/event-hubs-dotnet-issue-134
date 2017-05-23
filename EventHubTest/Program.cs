using System;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;

namespace EventHubTest
{
    class Program
    {
        static void Main(string[] args)
        {
            if (args.Length < 6)
            {
                Console.WriteLine("usage: EventHubTest <host> <keyName> <keyValue> <eventHubName> <partitionName> <consumerGroup>, [<offset>]");
            }
            else
            {
                try
                {
                    using (var cts = new CancellationTokenSource())
                    {
                        Console.CancelKeyPress += (_, ev) =>
                        {
                            ev.Cancel = true;
                            cts.Cancel();
                        };

                        ProcessAsync(args, cts.Token).GetAwaiter().GetResult();
                    }
                    Console.WriteLine("Finished ok");

                }
                catch (Exception ex)
                {
                    Console.WriteLine($"Caught error: {ex.Message}");
                }
            }
            if (Debugger.IsAttached)
            {
                Debugger.Break();
            }
        }

        private static async Task ProcessAsync(string[] args, CancellationToken ct)
        {
            var host = args[0];
            var keyName = args[1];
            var keyValue = args[2];
            var eventHubName = args[3];
            var partitionName = args[4];
            var consumerGroup = args[5];
            var latestOffset = (args.Length > 6) ? args[6] : null;
            var downloader = new LineDownloader(host, keyName, keyValue, eventHubName, partitionName, consumerGroup, latestOffset);
            while (true)
            {
                ct.ThrowIfCancellationRequested();
                var (offset, messagesReceived) = await downloader.TryDownloadLinesAsync(TimeSpan.FromSeconds(30), ct).ConfigureAwait(false);
                if (offset == null)
                {
                    Console.Write('-');
                }
                else
                {
                    Console.Write($"{messagesReceived}.");
                }
            }
        }
    }
}
