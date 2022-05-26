# MQTT Listener

## Prerequisites
- The port on which MQTT is listening should be accessible
- Visual Studio
- [XMPro IoT Framework NuGet package](https://www.nuget.org/packages/XMPro.IOT.Framework/)
- Please see the [Manage Agents](https://documentation.xmpro.com/how-tos/manage-agents) guide for a better understanding of how the Agent Framework works

## Description
The *MQTT listener* allows a user to receive data from a device or any source that uses the MQTT messaging protocol.

## How the code works
All settings referred to in the code need to correspond with the settings defined in the template that has been created for the agent using the XMPro Package Manager. Refer to the [XMPro Package Manager](https://documentation.xmpro.com/agent/packaging-agents/) guide for instructions on how to define the settings in the template and package the agent after building the code. 

After packaging the agent, you can upload it to the XMPro Data Stream Designer and start using it.

### Settings
When a user needs to use the *MQTT Listener*, they need to provide a Broker address, and a topic. A user also needs to specify the payload format. Available payload formats include JSON and HEX. Retrieve these values from the configuration using the following code: 

```csharp
private Configuration config;
private MqttClient client;
private string Broker => this.config["Broker"];
private string Topic => this.IsRemote() ? this.FromId().ToString() : this.config["Topic"];
private string Format => this.config["Format"] ?? "JSON";
```

The user also needs to specify a payload definition. Each definition consists of a name and data type, which is added via a grid. Get the value of the grid, using the following code:

```csharp
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
```

If the payload has a nested structure, the user needs to select the *Specify a JSON path for payload* check box. When a user clicks the *Add*-button on the grid, they will normally be allowed to only add a *Name* and *Type*, unless the *Specify JSON path for payload* check box is checked, for which an extra column will be added to allow the user to add the JSON path. To get the value of this setting, use the following code:

```csharp
private bool SpecifyJPath
{
    get
    {
        var specifyJPath = false;
        Boolean.TryParse(this.config["SpecifyJPath"], out specifyJPath);
        return specifyJPath;
    }
}
```

Example of a payload with a nested structure: Given the payload below, the *JSON Path* to the "*author*" attribute would be "*store.book.author*".

```json
{
  "store":
  {
    "book":
    {
      "category": "reference",
      "author": "Nigel Rees"
    }
  }
} 
```

### Configuration
In the *GetConfigurationTemplate* method, parse the JSON representation of the settings into the Settings object.

```csharp
var settings = Settings.Parse(template);
new Populator(parameters).Populate(settings);
```

Create controls for the items listed below and set their values and visibility.
* Topic 
* *Specify JSON path for payload* check box
* *Sample JSON Path* title 
* *Payload Definition* grid
* *JSON Path* text box
* *Payload format* drop down

```csharp
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
```

### Validate
The settings listed below should not be left empty. If they're left empty, an error needs to be added when the stream is validated.
* Broker Address
* Topic

```csharp
int i = 1;
var errors = new List<string>();
this.config = new Configuration() { Parameters = parameters };

if (String.IsNullOrWhiteSpace(this.Broker))
    errors.Add($"Error {i++}: Broker is not specified.");

if (String.IsNullOrWhiteSpace(this.Topic))
    errors.Add($"Error {i++}: Topic is not specified.");      
```

The Payload Definition should also contain at least one row. If the grid contains at least one row, make sure that, if the user checked the *Specify JSON path for payload* check box, the JSON path is defined.

```csharp
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
```

### Create
Set the config variable to the configuration received in the *Create* method.

```csharp
this.config = configuration;
```

Create a new MQTT client, given the value in the *Broker Address* field 

```csharp
this.client = new MqttClient(this.Broker);
```

Specify the event handler that should be used when the *MqttMsgPublishReceived* event is raised. This event will be raised each time a message is published on the topic that the client is subscribed to.

```csharp
this.client.MqttMsgPublishReceived += Client_MqttMsgPublishReceived;
```

### Start
In the *Start* method, connect to the MQTT client, providing a new and unique ID and then subscribe to the topic the user provided.

```csharp
public void Start()
{
    if (this.client.IsConnected == false)
    {
        this.client.Connect(Guid.NewGuid().ToString());
        this.client.Subscribe(new string[] { this.Topic }, new byte[] { MqttMsgBase.QOS_LEVEL_EXACTLY_ONCE });
    }
}
```

### Destroy
Make sure that the MQTT client is disconnected in the *Destroy* method.

```csharp
public void Destroy()
{
    if (this.client?.IsConnected == true)
        this.client.Disconnect();
}
```

### Publishing Events
Events need to be published in the event handler of the *MqttMsgPublishReceived* event.

If the format of the payload is **JSON**, first make sure that the message is correctly formatted. 

 ```csharp
var message = Encoding.UTF8.GetString(e.Message);
if (!message.StartsWith("[") || !message.EndsWith("]"))
    message = "[" + message.TrimStart('[').TrimEnd(']') + "]";
```

If there isn't a JSON path specified for the payload, the output of the agent would be the same as the input. However, if a path has been specified, it needs to apply the *SelectToken* to get the value, after which the value needs to be added to the output.

```csharp
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
```

If the format of the payload is **HEX**, for each of the rows in the payload definition, make sure the value of the data is in the correct format for each row and add the row to an object of type *JObject*. Finally, add the object to the output.

```csharp
JObject hObj = new JObject();
foreach (var row in this.PayloadDefinition.Rows)
{
    string value = String.Concat(ParseIndexes(row["ByteIndexes"].ToString(), e.Message.Length - 1).Select(i => e.Message[i].ToString("X2")));
    hObj.Add(new JProperty(row["Name"].ToString(), value));
}
output.Add(hObj);
```

Invoke the *OnPublish* event and publish the output to the *Output* endpoint.

```csharp
this.OnPublish?.Invoke(this, new OnPublishArgs(output, "Output"));
```

If an exception occurs, invoke the *OnPublish* event in the *catch* statement and publish the error details to the *Error* endpoint.

```csharp
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
```

### Decrypting Values
This agent does not have any values that need to be decrypted.