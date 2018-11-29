using Newtonsoft.Json.Linq;
using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Linq;
using System.Security.Cryptography.X509Certificates;
using System.Text;
using uPLibrary.Networking.M2Mqtt;
using uPLibrary.Networking.M2Mqtt.Messages;
using XMIoT.Framework;
using XMIoT.Framework.Settings;
using XMIoT.Framework.Settings.Enums;

namespace XMPro.MQTTAgents
{
    public class ListenerAdvanced : IAgent, IRemoteAgent
    {
        #region Static
        internal static X509Certificate2 LoadCertificate(File certFile, string certificatePassword) => new X509Certificate2(certFile.Content, certificatePassword);
        #endregion
        #region Constants
        private const string InputEndpoint = "Input";

        private const string BrokerParam = "Broker";
        private const string BrokerPortParam = "Broker_Port";
        private const string TopicParam = "Topic";
        private const string ClientIdParam = "ClientId";
        private const string QOSParam = "QOS";
        private const string IsBatchParam = "IsBatch";

        private const string SecureParam = "Secure";
        private const string SecureProtocolParam = "Protocol";
        private const string CACertParam = "CACert";
        private const string ClientCertParam = "ClientCert";
        private const string CertPasswordParam = "CertPassword";

        private const string AuthUsernameParam = "Username";
        private const string AuthPasswordParam = "Password";

        private const string PayloadParam = "PayloadDefinition";
        private const string PayloadNameColumn = "Name";
        private const string PayloadPathColumn = "Path";
        private const string PayloadTypeColumn = "Type";
        #endregion Constants
        private Configuration config;
        private MqttClient client;
        private string Broker => this.config[BrokerParam];
        private string Topic => this.IsRemote() ? this.FromId().ToString() : this.config[TopicParam];
        private byte QualityOfService { get; set; }
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

            this.client = CreateClient();

            this.client.MqttMsgPublishReceived += Client_MqttMsgPublishReceived;

            MqttClient CreateClient()
            {
                X509Certificate2 caCert = null;
                X509Certificate2 clientCert = null;
                MqttSslProtocols protocol = MqttSslProtocols.None;

                bool secure = bool.TryParse(this.config[SecureParam], out secure) && secure;
                if (secure)
                {
                    //MQTT can be made secure by a certificate from a Certificate Authority alone, or a CA certificate and a client certificate
                    //The CA certificate should never need a password
                    caCert = LoadCertificate(File.Parse(this.config[CACertParam]), string.Empty);

                    //The Client certificate should always require a password
                    if (!string.IsNullOrWhiteSpace(this.config[ClientCertParam]))
                    {
                        OnDecryptRequestArgs certDecryptArgs = new OnDecryptRequestArgs(this.config[CertPasswordParam]);
                        OnDecryptRequest(this, certDecryptArgs);
                        string clientPassword = certDecryptArgs.DecryptedValue;
                        clientCert = LoadCertificate(File.Parse(this.config[ClientCertParam]), clientPassword);
                    }

                    if (Enum.TryParse(this.config[SecureProtocolParam], out MqttSslProtocols parsedProtocol))
                        protocol = parsedProtocol;
                }

                return new MqttClient(
                    this.config[BrokerParam]
                    , Int32.TryParse(this.config[BrokerPortParam], out int port) && port > 0 ? port : 1883
                    , secure
                    , caCert
                    , clientCert
                    , protocol
                    , null);
            }
        }

        public void Start()
        {
            if (this.client.IsConnected == false)
            {
                string clientId = string.IsNullOrWhiteSpace(this.config[ClientIdParam]) ? Guid.NewGuid().ToString() : this.config[ClientIdParam];
                string authUsername = this.config[AuthUsernameParam];
                string authPassword = null;

                if (!string.IsNullOrWhiteSpace(this.config[AuthPasswordParam]))
                {
                    OnDecryptRequestArgs authDecryptArgs = new OnDecryptRequestArgs(this.config[AuthPasswordParam]);
                    OnDecryptRequest(this, authDecryptArgs);
                    authPassword = authDecryptArgs.DecryptedValue;
                }

                this.client.Connect(
                    clientId
                    , string.IsNullOrWhiteSpace(authUsername) ? null : authUsername
                    , string.IsNullOrWhiteSpace(authUsername) ? null : authPassword);
                this.client.Subscribe(new string[] { this.Topic }, new byte[] { (byte)Enum.Parse(typeof(QualityOfService), this.config[QOSParam]) });
            }
        }

        private void Client_MqttMsgPublishReceived(object sender, MqttMsgPublishEventArgs e)
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
                            string value = String.Concat(Listener.ParseIndexes(row["ByteIndexes"].ToString(), e.Message.Length - 1).Select(i => e.Message[i].ToString("X2")));
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
            var settingsObj = Settings.Parse(template);
            new Populator(parameters).Populate(settingsObj);
            var topic = (TextBox)settingsObj.Find(TopicParam);
            topic.Visible = !this.IsRemote();

            // MQTT
            var Format = settingsObj.Find("Format") as DropDown;
            CheckBox secure = (CheckBox)settingsObj.Find(SecureParam);
            FileUpload caCert = (FileUpload)settingsObj.Find(CACertParam);
            FileUpload clientCert = (FileUpload)settingsObj.Find(ClientCertParam);
            TextBox certPassword = (TextBox)settingsObj.Find(CertPasswordParam);
            DropDown protocol = (DropDown)settingsObj.Find(SecureProtocolParam);

            //Only show/require certificate fields if necessary
            caCert.Visible
                = caCert.Required
                = clientCert.Visible
                = certPassword.Visible
                = protocol.Visible
                = secure.Value;

            if (!secure.Value)
            {
                caCert.Value = null;
                clientCert.Value = null;
            }

            TextBox brokerUserName = (TextBox)settingsObj.Find(AuthUsernameParam);
            TextBox brokerPassword = (TextBox)settingsObj.Find(AuthPasswordParam);

            var SpecifyJPath = settingsObj.Find("SpecifyJPath") as CheckBox;
            SpecifyJPath.Visible = Format.Value == "JSON";
            var SampleJPath = settingsObj.Find("SampleJPath") as Title;
            var PayloadDefinition = settingsObj.Find("PayloadDefinition") as Grid;
            TextBox JPath = PayloadDefinition.Columns.First(s => s.Key == "Path") as TextBox;
            JPath.Visible
                = JPath.Required
                = SampleJPath.Visible
                = SpecifyJPath.Value && SpecifyJPath.Visible;
            DropDown Type = PayloadDefinition.Columns.First(s => s.Key == "Type") as DropDown;
            Type.Visible = Format.Value == "JSON";
            TextBox ByteIndexes = PayloadDefinition.Columns.First(s => s.Key == "ByteIndexes") as TextBox;
            ByteIndexes.Visible = Format.Value == "HEX";

            return settingsObj.ToString();
        }

        public string[] Validate(IDictionary<string, string> parameters)
        {
            List<string> errors = new List<string>();
            this.config = new Configuration() { Parameters = parameters };

            if (!parameters.Keys.Contains(BrokerParam) || string.IsNullOrWhiteSpace(parameters[BrokerParam]))
            {
                errors.Add("No broker address is set.");
            }

            if (!parameters.Keys.Contains(TopicParam) || string.IsNullOrWhiteSpace(parameters[TopicParam]))
            {
                errors.Add("No broker channel is set.");
            }

            if (parameters.Keys.Contains(SecureParam))
            {
                if (bool.TryParse(parameters[SecureParam], out bool secure) && secure)
                {
                    if (string.IsNullOrWhiteSpace(parameters[CACertParam]))
                    {
                        errors.Add("Secure channel requested, but no Certificate Authority's certificate has been attached.");
                    }
                }
            }

            if (parameters.Keys.Contains(AuthPasswordParam) && !string.IsNullOrWhiteSpace(parameters[AuthPasswordParam])
                && (!parameters.Keys.Contains(AuthUsernameParam) || string.IsNullOrWhiteSpace(parameters[AuthUsernameParam])))
            {
                errors.Add("Authentication password present, but username is not defined.");
            }

            if(!parameters.Keys.Contains(PayloadParam) || string.IsNullOrWhiteSpace(parameters[PayloadParam]) || !new Grid() { Value = parameters[PayloadParam] }.Rows.Any())
            {
                errors.Add("Payload is not defined.");
            }
            else
            {
                var grid = new Grid() { Value = parameters[PayloadParam] };
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
            return this.PayloadDefinition.Rows.Select(row => new XMIoT.Framework.Attribute((string)row[PayloadNameColumn], this.Format == "JSON" ? (Types)Enum.Parse(typeof(Types), (string)row[PayloadTypeColumn]) : Types.String));

        }
    }
}
