using System;
using System.Text;
using System.Collections.Generic;
using Newtonsoft.Json;
using Microsoft.Azure.EventHubs;
using Microsoft.Azure.WebJobs.ServiceBus;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Host;

namespace processDeviceTelemetry
{
    public static class processSensorsTelemetry
    {
        

        [FunctionName("processSensorsTelemetry")]        
        public static void Run([EventHubTrigger("%IoTHubName%", Connection = "EventHubConnection", ConsumerGroup = "%ConsumerGroup%")] EventData deviceIoTHubMessage, 
            [Table("%TelemetryTableName%", Connection = "TelemetryTableConnection")] ICollector<DeviceNotification> tableBinding,
            TraceWriter log)
        {
            try
            {
                if (deviceIoTHubMessage == null ||
                    (deviceIoTHubMessage.Body == null || deviceIoTHubMessage.Body.Array == null))
                    return;

                var DeviceID = deviceIoTHubMessage.Properties["iothub-connection-device-id"] as string;
                var sMessageId = deviceIoTHubMessage.Properties["iothub-connection-auth-generation-id"] as string;

                log.Info(String.Format("device: {0} message-id: {1}", DeviceID, sMessageId));
                string jsonBody = Encoding.UTF8.GetString(deviceIoTHubMessage.Body.Array);
                log.Info(jsonBody);

                PoseidonMessage msg = JsonConvert.DeserializeObject<PoseidonMessage>(jsonBody);
                
                msg.PersistNotificationsList(tableBinding, deviceIoTHubMessage.SystemProperties.EnqueuedTimeUtc, DeviceID, sMessageId);
                
            }
            catch(Exception ex)
            {
                log.Error("Processing error: ", ex);
                return;
            }
        }

        private static string GetEnvironmentVariable(string name)
        {
            return System.Environment.GetEnvironmentVariable(name, EnvironmentVariableTarget.Process);
        }
       
    }


    public class DeviceNotification
    {
        public string PartitionKey { get; set; }
        public string RowKey { get; set; }
        public string AuthGenerationId { get; set; }
        public string DeviceId { get; set; }
        public string Topic { get; set; }
        public int SensorId { get; set; }        
        public DateTime EnqueuedTimeUtc { get; set; }
        public string ValueLabel { get; set; }
        /*public string ValueUnits { get; set; }*/        
        public double Value { get; set; }

    }

    /***
     * Poseidon device Message structure
     */
    public class PoseidonMessage
    {
        public SensorData[] sensors { get; set; }
        public SensorData[] inputs { get; set; }
        public SensorData[] outputs { get; set; }

        internal void PersistNotificationsList(ICollector<DeviceNotification> tableBinding, DateTime EnqueuedTimeUtc, string DeviceId, string sMessageId)
        {
            PersistTopicData(tableBinding, sensors, EnqueuedTimeUtc, DeviceId, sMessageId, PoseidonTopic.Sensors);
            PersistTopicData(tableBinding, inputs, EnqueuedTimeUtc, DeviceId, sMessageId, PoseidonTopic.Inputs);
            PersistTopicData(tableBinding, outputs, EnqueuedTimeUtc, DeviceId, sMessageId, PoseidonTopic.Outputs);            
        }

        private void PersistTopicData(ICollector<DeviceNotification> tableBinding, SensorData[] sensorsData, DateTime EnqueuedTimeUtc, string DeviceId, string sMessageId, PoseidonTopic topic)
        {
            if (sensorsData == null || sensorsData.Length == 0)
                return;
           
            List<DeviceNotification> returnValue = new List<DeviceNotification>(sensorsData.Length);

            foreach(SensorData sensorData in sensorsData) {
                if (sensorData != null)
                {
                    tableBinding.Add(new DeviceNotification
                    {
                        PartitionKey = EnqueuedTimeUtc.ToString("yyyy-MM"),
                        RowKey = Guid.NewGuid().ToString(),
                        AuthGenerationId = sMessageId,
                        DeviceId = DeviceId,
                        Topic = Enum.GetName(typeof(PoseidonTopic), topic),
                        EnqueuedTimeUtc = EnqueuedTimeUtc,
                        SensorId = sensorData.id,
                        ValueLabel = sensorData.name,
                        Value = sensorData.value
                    });
                }
              }
            return;
        }
    }

    public class SensorData
    {
        public int id { get; set; }
        public string name { get; set; }
        public double value { get; set; }
        public int state { get; set; }
    }

    public enum PoseidonTopic
    {
        Sensors,
        Inputs,
        Outputs
    }
}
