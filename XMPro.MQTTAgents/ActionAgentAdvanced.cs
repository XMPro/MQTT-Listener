using System;
using System.Collections.Generic;
using System.Linq;
using System.Security.Cryptography.X509Certificates;
using System.Text;
using Newtonsoft.Json.Linq;
using uPLibrary.Networking.M2Mqtt;
using uPLibrary.Networking.M2Mqtt.Messages;
using XMIoT.Framework;
using XMIoT.Framework.Settings;

namespace XMPro.MQTTAgents
{
    enum QualityOfService : byte
    {
        AtMostOnce = MqttMsgBase.QOS_LEVEL_AT_MOST_ONCE,
        AtLeastOnce = MqttMsgBase.QOS_LEVEL_AT_LEAST_ONCE,
        ExactlyOnce = MqttMsgBase.QOS_LEVEL_EXACTLY_ONCE
    }
    public class ActionAgentAdvanced : IAgent, IRemoteAgent, IReceivingAgent
    {
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

        private const string AuthAnonymousParam = "Anonymous";
        private const string AuthUsernameParam = "Username";
        private const string AuthPasswordParam = "Password";

        private const string NestedObjectGroup = "NestedObject";
        private const string UseNestedObjectParam = "UseNestedObject";
        private const string ObjectNameParam = "ObjectName";
        private const string OutputAsArrayParam = "OutputAsArray";
        private const string ObjectPropertiesParam = "ObjectProperties";
        private const string SourceNameColumn = "Source";
        private const string OutputNameColumn = "Alias";
        #endregion Constants

        private Configuration config;
        private MqttClient client;
        private string Broker => this.config[BrokerParam];
        private string Topic => this.IsRemote() ? this.FromId().ToString() : this.config[TopicParam];
        private bool IsBatch => bool.TryParse(this.config[IsBatchParam], out bool isBatch) ? isBatch : true;
        private byte QualityOfService { get; set; }

        private bool UseNestedObject => bool.TryParse(this.config[UseNestedObjectParam], out bool nestedObject) ? nestedObject : false;
        private bool NestedObjectAsArray => bool.TryParse(this.config[OutputAsArrayParam], out bool outputArray) ? outputArray : false;
        private IReadOnlyCollection<Tuple<string, string>> ComplexObjectAliases { get; set; } = Array.Empty<Tuple<string, string>>();
        private string ClientID => string.IsNullOrWhiteSpace(this.config[ClientIdParam]) ? Guid.NewGuid().ToString() : this.config[ClientIdParam];
        private string AuthUsername => this.config[AuthUsernameParam];
        private string AuthPassword
        {
            get
            {
                string authPassword = null;
                if (!string.IsNullOrWhiteSpace(this.config[AuthPasswordParam]))
                {
                    authPassword = decrypt(this.config[AuthPasswordParam]);
                }
                return authPassword;
            }            
        }

        public long UniqueId { get; set; }
        public event EventHandler<OnPublishArgs> OnPublish;
        public event EventHandler<OnRequestParentOutputAttributesArgs> OnRequestParentOutputAttributes;
        public event EventHandler<OnDecryptRequestArgs> OnDecryptRequest;

        private string decrypt(string value)
        {
            var request = new OnDecryptRequestArgs(value);
            this.OnDecryptRequest?.Invoke(this, request);
            return request.DecryptedValue;
        }

        public void Create(Configuration configuration)
        {
            this.config = configuration;
            QualityOfService = (byte)Enum.Parse(typeof(QualityOfService), this.config[QOSParam]);
            this.client = CreateClient();

            if (UseNestedObject)
            {
                Grid coGrid = new Grid
                {
                    Value = this.config[ObjectPropertiesParam]
                };

                //Store source/alias names for the complex object
                ComplexObjectAliases = coGrid.Rows.Select(r =>
                {
                    string source = (string)r[SourceNameColumn];
                    string alias = (string)r[OutputNameColumn];
                    alias = string.IsNullOrWhiteSpace(alias) ? source : alias;
                    return new Tuple<string, string>(source, alias);
                }).ToArray();
            }

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
                    caCert = ListenerAdvanced.LoadCertificate(File.Parse(this.config[CACertParam]), string.Empty);

                    //The Client certificate should always require a password
                    if (!string.IsNullOrWhiteSpace(this.config[ClientCertParam]))
                    {
                        OnDecryptRequestArgs certDecryptArgs = new OnDecryptRequestArgs(this.config[CertPasswordParam]);
                        OnDecryptRequest(this, certDecryptArgs);
                        string clientPassword = certDecryptArgs.DecryptedValue;
                        clientCert = ListenerAdvanced.LoadCertificate(File.Parse(this.config[ClientCertParam]), clientPassword);
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

            bool isRemote = this.IsRemote();
            TextBox topic = (TextBox)settingsObj.Find(TopicParam);
            topic.Visible = topic.Required = !isRemote;

            if (!isRemote)
            {
                //Complex object
                CheckBox nestedObject = (CheckBox)settingsObj.Find(UseNestedObjectParam);

                TextBox objectName = (TextBox)settingsObj.Find(ObjectNameParam);
                CheckBox outputAsArray = (CheckBox)settingsObj.Find(OutputAsArrayParam);
                Grid objectProperties = (Grid)settingsObj.Find(ObjectPropertiesParam);

                //Only show/require complex object fields if necessary
                outputAsArray.Visible
                    = objectName.Visible
                    = objectName.Required
                    = objectProperties.Visible
                    = objectProperties.Required
                    = nestedObject.Value;

                //The source for the complex object is the inputs to this stream object
                DropDown sourceColumn = (DropDown)objectProperties.Columns.First(s => s.Key == SourceNameColumn);
                sourceColumn.Options = this.GetInputAttributes(InputEndpoint, parameters)
                        .Select(l => new Option() { DisplayMemeber = l.Name, ValueMemeber = l.Name })
                        .ToList();

                //Do not show the alias column if the object will be an array
                TextBox nameColumn = (TextBox)objectProperties.Columns.First(s => s.Key == OutputNameColumn);
                nameColumn.Visible = !outputAsArray.Value;
            }
            else
            {
                //The stream object is a remote agent, do not show the nested object fields at all
                Group nestedObject = (Group)settingsObj.Find(NestedObjectGroup);
                nestedObject.Visible = false;
            }

            // MQTT
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

            CheckBox anonymous = (CheckBox)settingsObj.Find(AuthAnonymousParam);
            TextBox brokerUserName = (TextBox)settingsObj.Find(AuthUsernameParam);
            TextBox brokerPassword = (TextBox)settingsObj.Find(AuthPasswordParam);

            //Only show/require authentication info if necessary
            brokerUserName.Visible
                = brokerUserName.Required
                = brokerPassword.Visible
                = !anonymous.Value;

            return settingsObj.ToString();
        }

        public string[] Validate(IDictionary<string, string> parameters)
        {
            List<string> errors = new List<string>();

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
            return errors.ToArray();
        }

        public IEnumerable<XMIoT.Framework.Attribute> GetInputAttributes(string endpoint, IDictionary<string, string> parameters)
        {
            var args = new OnRequestParentOutputAttributesArgs(this.UniqueId, InputEndpoint);
            this.OnRequestParentOutputAttributes.Invoke(this, args);
            return args.ParentOutputs;
        }

        public IEnumerable<XMIoT.Framework.Attribute> GetOutputAttributes(string endpoint, IDictionary<string, string> parameters)
        {
            var inputAttributes = this.GetInputAttributes(InputEndpoint, parameters);

            if (!(parameters.Keys.Contains(UseNestedObjectParam) && bool.TryParse(parameters[UseNestedObjectParam], out bool useNestedObject) && useNestedObject))
                return inputAttributes;

            var complexObjectGrid = new Grid
            {
                Value = this.config[ObjectPropertiesParam]
            };

            //Remove fields that are going into the complex object, if any
            //Downstream objects are not informed of the complex object field
            return inputAttributes.Where(a => complexObjectGrid.Rows.All(m => ((string)m[SourceNameColumn]) != a.Name));
        }

        public void Receive(string endpointName, JArray events)
        {
            JArray processedArray;

            if (this.client.IsConnected == false)
                this.client.Connect(ClientID, AuthUsername, AuthPassword);

            if (this.client.IsConnected == true)
            {
                if (!IsBatch)
                {
                    //Process and transmit all records individually
                    processedArray = new JArray();
                    foreach (JObject inputObject in events.Cast<JObject>())
                    {
                        JObject processedObject = UseNestedObject ? ProcessComplexObject(inputObject) : inputObject;
                        client.Publish(this.Topic, Encoding.UTF8.GetBytes(processedObject.ToString()), QualityOfService, false);
                        processedArray.Add(processedObject);
                    }
                }
                else
                {
                    //Process and transmit all records at once
                    processedArray = UseNestedObject ? JArray.FromObject(events.Cast<JObject>().Select(inputObject => ProcessComplexObject(inputObject))) : events;
                    client.Publish(this.Topic, Encoding.UTF8.GetBytes(processedArray.ToString()), QualityOfService, false);
                }

                //Always send all records to the next stream object at once
                this.OnPublish?.Invoke(this, new OnPublishArgs(processedArray, "Output"));
            }

            JObject ProcessComplexObject(JObject inputObject)
            {
                JObject dataRecord = new JObject(inputObject);
                Dictionary<string, object> outputDict = new Dictionary<string, object>();

                //Remove requested fields from the record and attach to a temporary dictionary
                foreach (var (source, alias) in ComplexObjectAliases)
                {
                    outputDict.Add(alias, dataRecord[source]);
                    dataRecord.Remove(source);
                }

                //Turn the dictionary into the requested complex object (either an object or an array)
                JToken complexObject = NestedObjectAsArray ? (JToken)JArray.FromObject(outputDict.Values) : JObject.FromObject(outputDict);
                dataRecord.Add(this.config[ObjectNameParam], complexObject);

                return dataRecord;
            }
        }
    }
}