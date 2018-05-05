using System;
using System.Configuration;
using System.Threading.Tasks;
using System.Text;
using System.Net;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Host;
using System.Net.Http;
using System.IO;
using Microsoft.Azure.ServiceBus;

namespace wetherImporter
{
    public static class WetherTimerImport
    {
        /// <summary>
        /// 気象庁1時間ごとの降水量CSV取得URL
        /// </summary>
        static readonly String url = "http://www.data.jma.go.jp/obd/stats/data/mdrr/pre_rct/alltable/pre1h00_rct.csv";

        /// <summary>
        /// HttpClient
        /// </summary>
        static readonly HttpClient client = new HttpClient();

        /// <summary>
        /// CSVファイル文字コード
        /// </summary>
        static readonly Encoding encode = Encoding.GetEncoding("Shift_JIS");

        /// <summary>
        /// Service Bus QueueClient;
        /// </summary>
        static IQueueClient queueClient;

        /// <summary>
        /// Queue名称
        /// </summary>
        static readonly string queueName = "wetherQueue";

        [FunctionName("wetherImport")]
        public static async Task ImportWetherData([TimerTrigger("0 */10 * * * *")]TimerInfo timer, TraceWriter writer)
        {
            writer.Info($"Start Function : wetherImport at {DateTime.Now}");

            writer.Verbose($"Start download from kishouchou.");
            // 1時間当たりの降水量情報取得
            var response = await client.GetAsync(url);
            writer.Verbose($"Downloaded from kishouchou.");

            response.EnsureSuccessStatusCode();

            var serviceBusEndpoint = ConfigurationManager.AppSettings["AzureServiceBus"];
            writer.Verbose($"Service Bus Endpoin: {serviceBusEndpoint}, queueName: {queueName}");

            queueClient = new QueueClient(serviceBusEndpoint, queueName);

            // CSVデータの読込
            using (var stream = (await response.Content.ReadAsStreamAsync()))
            {
                using (var reader = new StreamReader(stream, encode, true))
                {
                    // 先頭行はヘッダーなので飛ばす
                    reader.ReadLine();

                    await Task.Run(async () =>
                    {
                        while (!reader.EndOfStream)
                        {
                            string dataLine = await reader.ReadLineAsync();
                            if (dataLine != null) {
                                var message = new Message(Encoding.UTF8.GetBytes(dataLine));
                                await queueClient.SendAsync(message);
                            } else {
                                break;
                            }
                        }
                    });
                }
            }
            writer.Info($"End Function : wetherImport at {DateTime.Now}");
        }
    }
}
