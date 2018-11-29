using Newtonsoft.Json.Linq;
using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Linq;
using System.Text;
using System.Text.RegularExpressions;
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
        private string Format => this.config["Format"] ?? "JSON";

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
            string message = null;
            try
            {
                JArray output = new JArray();

                switch (this.Format)
                {
                    case "JSON":                        
                        message = Encoding.UTF8.GetString(e.Message);
                        if (!message.StartsWith("[") || !message.EndsWith("]"))
                            message = "[" + message.TrimStart('[').TrimEnd(']') + "]";

                        JArray input = JArray.Parse(message);

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
                        break;                        
                    case "HEX":
                        JObject hObj = new JObject();
                        foreach (var row in this.PayloadDefinition.Rows)
                        {
                            string value = String.Concat(ParseIndexes(row["ByteIndexes"].ToString(), e.Message.Length - 1).Select(i => e.Message[i].ToString("X2")));
                            hObj.Add(new JProperty(row["Name"].ToString(), value));
                        }
                        output.Add(hObj);
                        break;
                    default:
                        throw new IndexOutOfRangeException($"Unknown payload format specified: {this.Format}");
                }

                this.OnPublish?.Invoke(this, new OnPublishArgs(output, "Output"));
            }
            catch (Exception ex)
            {
                JObject errorObject = new JObject()
                {
                    { "AgentId" , this.UniqueId },
                    { "Timestamp", DateTime.UtcNow },
                    { "Source", nameof(Client_MqttMsgPublishReceived) },
                    { "Error", ex.GetType().Name },
                    { "DetailedError", ex.Message },
                    { "Data", message ?? "Error deserializing the data"},
                };
                this.OnPublish?.Invoke(this, new OnPublishArgs(new JArray() { errorObject }, "Error"));
            }
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

            var Format = settings.Find("Format") as DropDown;

            var SpecifyJPath = settings.Find("SpecifyJPath") as CheckBox;
            SpecifyJPath.Visible = Format.Value == "JSON";
            var SampleJPath = settings.Find("SampleJPath") as Title;
            var PayloadDefinition = settings.Find("PayloadDefinition") as Grid;
            TextBox JPath = PayloadDefinition.Columns.First(s => s.Key == "Path") as TextBox;
            JPath.Visible 
                = JPath.Required 
                = SampleJPath.Visible 
                = SpecifyJPath.Value && SpecifyJPath.Visible;
            DropDown Type = PayloadDefinition.Columns.First(s => s.Key == "Type") as DropDown;
            Type.Visible = Format.Value == "JSON";
            TextBox ByteIndexes = PayloadDefinition.Columns.First(s => s.Key == "ByteIndexes") as TextBox;
            ByteIndexes.Visible = Format.Value == "HEX";
                
            
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
            this.config = new Configuration() { Parameters = parameters };
            
            foreach (var row in this.PayloadDefinition.Rows)
            {
                yield return new XMIoT.Framework.Attribute(row["Name"].ToString(), this.Format == "JSON" ? (Types)Enum.Parse(typeof(Types), row["Type"].ToString()) : Types.String);
            }
        }

        public static IEnumerable<Int32> ParseIndexes(String s, Int32 lastIndex)
        {
            String[] parts = s.Split(' ', ';', ',');
            Regex reRange = new Regex(@"^\s*((?<from>\d+)|(?<from>\d+)(?<sep>(-|\.\.))(?<to>\d+)|(?<sep>(-|\.\.))(?<to>\d+)|(?<from>\d+)(?<sep>(-|\.\.)))\s*$");
            foreach (String part in parts)
            {
                Match maRange = reRange.Match(part);
                if (maRange.Success)
                {
                    var gFrom = maRange.Groups["from"];
                    var gTo = maRange.Groups["to"];
                    var gSep = maRange.Groups["sep"];

                    if (gSep.Success)
                    {
                        Int32 from = 0;
                        Int32 to = lastIndex;
                        if (gFrom.Success)
                            from = Int32.Parse(gFrom.Value);
                        if (gTo.Success)
                            to = Int32.Parse(gTo.Value);
                        for (Int32 page = from; page <= to; page++)
                            yield return page;
                    }
                    else
                        yield return Int32.Parse(gFrom.Value);
                }
            }
        }
    }
}
