using System;
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
    public class WetherTimerImport
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
        public static async Task ImportWetherData([TimerTrigger("0 */20 * * * *")]TimerInfo timer, TraceWriter writer)
        {
            writer.Info($"Start Function : wetherImport at {DateTime.Now}");

            var response = await client.GetAsync(url);

            response.EnsureSuccessStatusCode();

            queueClient = new QueueClient("", queueName);

            using (var stream = (await response.Content.ReadAsStreamAsync()))
            {
                using (var reader = (new StreamReader(stream)) as TextReader)
                {
                    var header = reader.ReadLine();

                    string dataLine = await reader.ReadLineAsync();
                    if (dataLine != null) {
                        
                    }
                }
            }
            writer.Info($"End Function : wetherImport at {DateTime.Now}");
        }
    }
}
