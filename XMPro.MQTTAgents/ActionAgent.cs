using System;
using System.Collections.Generic;
using System.Text;
using Newtonsoft.Json.Linq;
using uPLibrary.Networking.M2Mqtt;
using uPLibrary.Networking.M2Mqtt.Messages;
using XMIoT.Framework;
using XMIoT.Framework.Settings;

namespace XMPro.MQTTAgents
{
    public class ActionAgent : IAgent, IRemoteAgent, IReceivingAgent
    {
        private Configuration config;
        private MqttClient client;
        private string Broker => this.config["Broker"];
        private string Topic => this.IsRemote() ? this.FromId().ToString() : this.config["Topic"];

        public long UniqueId { get; set; }

        public event EventHandler<OnPublishArgs> OnPublish;
        public event EventHandler<OnRequestParentOutputAttributesArgs> OnRequestParentOutputAttributes;
        public event EventHandler<OnDecryptRequestArgs> OnDecryptRequest;

        public void Create(Configuration configuration)
        {
            this.config = configuration;
            this.client = new MqttClient(this.Broker);
        }

        public void Start()
        {
        }

        public void Destroy()
        {
            if (this.client?.IsConnected == true)
                this.client.Disconnect();
        }

        public string GetConfigurationTemplate(string template, IDictionary<string, string> parameters)
        {
            var settings = Settings.Parse(template);
            new Populator(parameters).Populate(settings);
            var topic = settings.Find("Topic") as TextBox;
            topic.Visible = topic.Required = !this.IsRemote();
            return settings.ToString();
        }

        public string[] Validate(IDictionary<string, string> parameters)
        {
            int i = 1;
            var errors = new List<string>();
            this.config = new Configuration() { Parameters = parameters };

            if (String.IsNullOrWhiteSpace(this.Broker))
                errors.Add($"Error {i++}: Broker is not specified.");

            if (String.IsNullOrWhiteSpace(this.Topic))
                errors.Add($"Error {i++}: Topic is not specified.");

            return errors.ToArray();
        }

        public IEnumerable<XMIoT.Framework.Attribute> GetInputAttributes(string endpoint, IDictionary<string, string> parameters)
        {
            yield break;
        }

        public IEnumerable<XMIoT.Framework.Attribute> GetOutputAttributes(string endpoint, IDictionary<string, string> parameters)
        {
            var args = new OnRequestParentOutputAttributesArgs(this.UniqueId, "Input");
            this.OnRequestParentOutputAttributes.Invoke(this, args);
            return args.ParentOutputs;
        }

        public void Receive(string endpointName, JArray events)
        {
            if (this.client.IsConnected == false)
                this.client.Connect(Guid.NewGuid().ToString());

            if (this.client.IsConnected == true)
            {
                client.Publish(this.Topic, Encoding.UTF8.GetBytes(events.ToString()), MqttMsgBase.QOS_LEVEL_EXACTLY_ONCE, false);
                this.OnPublish?.Invoke(this, new OnPublishArgs(events, "Output"));
            }
        }
    }
}