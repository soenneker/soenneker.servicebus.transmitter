using System.Text.Json;
using System.Text.Json.Serialization;

namespace Audit;

[JsonSourceGenerationOptions(JsonSerializerDefaults.Web, DefaultIgnoreCondition = JsonIgnoreCondition.WhenWritingNull,
    ReadCommentHandling = JsonCommentHandling.Skip, UseStringEnumConverter = true)]
[JsonSerializable(typeof(Payload))]
internal partial class TestJsonContext : JsonSerializerContext
{
}
