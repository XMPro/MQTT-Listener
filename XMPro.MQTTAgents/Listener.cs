using Newtonsoft.Json.Linq;
using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Linq;
using System.Text;
using uPLibrary.Networking.M2Mqtt;
using uPLibrary.Networking.M2Mqtt.Messages;
using XMIoT.Framework;
using XMIoT.Framework.Settings;
using XMIoT.Framework.Settings.Enums;

namespace XMPro.MQTTAgents
{
    public class Listener : IAgent, IRemoteAgent
    {
        private Configuration config;
        private MqttClient client;
        private string Broker => this.config["Broker"];
        private string Topic => this.IsRemote() ? this.FromId().ToString() : this.config["Topic"];
        private Grid _PayloadDefinition;
        private Grid PayloadDefinition
        {
            get
            {
                if (this._PayloadDefinition == null)
                {
                    var grid = new Grid();
                    grid.Value = this.config["PayloadDefinition"];
                    this._PayloadDefinition = grid;
                }
                return this._PayloadDefinition;
            }
        }
        private bool SpecifyJPath
        {
            get
            {
                var specifyJPath = false;
                Boolean.TryParse(this.config["SpecifyJPath"], out specifyJPath);
                return specifyJPath;
            }
        }

        public long UniqueId { get; set; }

        public event EventHandler<OnPublishArgs> OnPublish;

        public event EventHandler<OnDecryptRequestArgs> OnDecryptRequest;

        public void Create(Configuration configuration)
        {
            this.config = configuration;
            this.client = new MqttClient(this.Broker);
            this.client.MqttMsgPublishReceived += Client_MqttMsgPublishReceived;
        }

        public void Start()
        {
            if (this.client.IsConnected == false)
            {
                this.client.Connect(Guid.NewGuid().ToString());
                this.client.Subscribe(new string[] { this.Topic }, new byte[] { MqttMsgBase.QOS_LEVEL_EXACTLY_ONCE });
            }
        }

        private void Client_MqttMsgPublishReceived(object sender, uPLibrary.Networking.M2Mqtt.Messages.MqttMsgPublishEventArgs e)
        {       
            var message = Encoding.UTF8.GetString(e.Message);
            if (!message.StartsWith("[") || !message.EndsWith("]"))
                message = "[" + message.TrimStart('[').TrimEnd(']') + "]";

            JArray input = JArray.Parse(message);
            JArray output = new JArray();

            if (SpecifyJPath == false)
            {
                output = input;
            }
            else
            {
                foreach (var obj in input.Children<JObject>())
                {
                    JObject modObj = new JObject();
                    foreach (var row in this.PayloadDefinition.Rows)
                    {
                        object value = ((JValue)obj.SelectToken(row["Path"].ToString()))?.Value;
                        modObj.Add(new JProperty(row["Name"].ToString(), value));
                    }
                    output.Add(modObj);
                }
            }

            this.OnPublish?.Invoke(this, new OnPublishArgs(output, "Output"));
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
            topic.Visible = !this.IsRemote();

            var SpecifyJPath = settings.Find("SpecifyJPath") as CheckBox;
            var SampleJPath = settings.Find("SampleJPath") as Title;
            var PayloadDefinition = settings.Find("PayloadDefinition") as Grid;
            TextBox JPath = PayloadDefinition.Columns.First(s => s.Key == "Path") as TextBox;
            JPath.Visible = JPath.Required = SampleJPath.Visible = SpecifyJPath.Value;

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

            var grid = new Grid();
            grid.Value = this.config["PayloadDefinition"];
            if (grid.Rows.Any() == false)
                errors.Add($"Error {i++}: Payload Definition is not specified.");
            else
            {
                int rowCount = 1;
                foreach (var row in grid.Rows)
                {
                    if (SpecifyJPath == true && String.IsNullOrEmpty(row["Path"].ToString()))
                        errors.Add("JSON Path is not specified on row " + rowCount);
                    rowCount++;
                }
            }

            return errors.ToArray();
        }

        public IEnumerable<XMIoT.Framework.Attribute> GetOutputAttributes(string endpoint, IDictionary<string, string> parameters)
        {
            var grid = new Grid();
            grid.Value = parameters["PayloadDefinition"];
            foreach (var row in grid.Rows)
            {
                yield return new XMIoT.Framework.Attribute(row["Name"].ToString(), (Types)Enum.Parse(typeof(Types), row["Type"].ToString()));
            }
        }
    }
}