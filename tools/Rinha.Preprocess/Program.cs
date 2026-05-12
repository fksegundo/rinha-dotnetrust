using System.IO.Compression;
using System.Text.Json;
using Rinha.Core;

if (args.Length < 2)
{
    Console.Error.WriteLine("Usage: Rinha.Preprocess <references.json.gz> <output.idx> [leafSize] or [native leafSize]");
    return 2;
}

string inputPath = args[0];
string outputPath = args[1];

int leafSize = 192; // default
if (args.Length > 2)
{
    if (string.Equals(args[2], "native", StringComparison.OrdinalIgnoreCase))
    {
        if (args.Length > 3 && int.TryParse(args[3], out int parsedLeafSize))
            leafSize = parsedLeafSize;
    }
    else if (int.TryParse(args[2], out int parsedLeafSize))
    {
        leafSize = parsedLeafSize;
    }
}

Console.WriteLine($"Reading {inputPath}");
byte[] json;
await using (var source = File.OpenRead(inputPath))
await using (var gzip = new GZipStream(source, CompressionMode.Decompress))
await using (var memory = new MemoryStream(capacity: 300 * 1024 * 1024))
{
    await gzip.CopyToAsync(memory);
    json = memory.ToArray();
}

Console.WriteLine($"Parsing {json.Length / 1024 / 1024} MiB of references");
var (flatVectors, flatLabels, flatCount) = ParseFlatReferences(json);
Console.WriteLine($"Parsed {flatCount:n0} references");

Console.WriteLine($"Building native exact tree index with leaf size {leafSize}");
NativeIndexWriter.Write(outputPath, flatVectors, flatLabels, flatCount, leafSize);
Console.WriteLine("Done");
return 0;

static (short[] Vectors, byte[] Labels, int Count) ParseFlatReferences(byte[] json)
{
    const int initialCapacity = 3_000_000;
    var vectors = new short[initialCapacity * VectorSpec.PackedDimensions];
    var labels = new byte[initialCapacity];
    int count = 0;
    int vectorPosition = -1;
    bool inVector = false;
    bool expectingLabel = false;

    var reader = new Utf8JsonReader(json.AsSpan(), isFinalBlock: true, state: default);
    while (reader.Read())
    {
        switch (reader.TokenType)
        {
            case JsonTokenType.PropertyName:
                inVector = false;
                expectingLabel = false;
                if (reader.ValueSpan.SequenceEqual("vector"u8))
                    vectorPosition = 0;
                else if (reader.ValueSpan.SequenceEqual("label"u8))
                    expectingLabel = true;
                break;

            case JsonTokenType.StartArray when vectorPosition == 0:
                inVector = true;
                break;

            case JsonTokenType.Number when inVector:
                EnsureFlatCapacity(ref vectors, ref labels, count);
                if (vectorPosition < VectorSpec.Dimensions)
                    vectors[count * VectorSpec.PackedDimensions + vectorPosition] = VectorSpec.QuantizeFlat(reader.GetDouble());
                vectorPosition++;
                break;

            case JsonTokenType.EndArray when inVector:
                inVector = false;
                break;

            case JsonTokenType.String when expectingLabel:
                EnsureFlatCapacity(ref vectors, ref labels, count);
                labels[count] = reader.ValueSpan.SequenceEqual("fraud"u8) ? (byte)1 : (byte)0;
                expectingLabel = false;
                count++;
                break;
        }
    }

    Array.Resize(ref vectors, count * VectorSpec.PackedDimensions);
    Array.Resize(ref labels, count);
    return (vectors, labels, count);
}

static void EnsureFlatCapacity(ref short[] vectors, ref byte[] labels, int count)
{
    if (count < labels.Length)
        return;

    int newCapacity = labels.Length + labels.Length / 2;
    Array.Resize(ref labels, newCapacity);
    Array.Resize(ref vectors, newCapacity * VectorSpec.PackedDimensions);
}
