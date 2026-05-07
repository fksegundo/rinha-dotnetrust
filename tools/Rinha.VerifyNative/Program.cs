using System.Buffers.Binary;
using System.IO.Compression;
using System.Text;
using System.Text.Json;
using Rinha.Core;

if (args.Length < 3)
{
    Console.Error.WriteLine("Usage: Rinha.VerifyNative <references.json|references.json.gz> <test-data.json> <native.idx> [sampleCount]");
    return 2;
}

string referencesPath = args[0];
string testDataPath = args[1];
string nativeIndexPath = args[2];
int sampleCount = args.Length > 3 ? int.Parse(args[3]) : 32;

byte[] nativeIndexBytes = await File.ReadAllBytesAsync(nativeIndexPath);
var nativeFlat = LoadNativeFlat(nativeIndexBytes);

Console.WriteLine($"Loading references: {referencesPath}");
byte[] referencesJson = await ReadInputAsync(referencesPath);
var (vectors, labels, referenceCount) = ParseFlatReferences(referencesJson);
Console.WriteLine($"Loaded {referenceCount:n0} reference vectors");

Console.WriteLine($"Loading test data: {testDataPath}");
byte[] testDataJson = await File.ReadAllBytesAsync(testDataPath);
using JsonDocument document = JsonDocument.Parse(testDataJson);
JsonElement.ArrayEnumerator entries = document.RootElement.GetProperty("entries").EnumerateArray();
JsonElement[] allEntries = [.. entries];
sampleCount = Math.Clamp(sampleCount, 1, allEntries.Length);

using var native = NativeFraudSearch.Open(nativeIndexPath);

int exactVsExpectedMismatches = 0;
int nativeFileVsExactMismatches = 0;
int nativeVsExactMismatches = 0;
int nativeVsExpectedMismatches = 0;

Console.WriteLine($"Verifying {sampleCount} sampled requests");
Span<short> query = stackalloc short[VectorSpec.PackedDimensions];
for (int sample = 0; sample < sampleCount; sample++)
{
    int entryIndex = sample * allEntries.Length / sampleCount;
    JsonElement entry = allEntries[entryIndex];
    JsonElement request = entry.GetProperty("request");
    string requestJson = request.GetRawText();
    byte[] payload = Encoding.UTF8.GetBytes(requestJson);

    if (!FraudVectorizer.TryVectorizeFlat(payload, query))
    {
        Console.WriteLine($"[{sample + 1}/{sampleCount}] vectorization failed for {entry.GetProperty("request").GetProperty("id").GetString()}");
        continue;
    }

    int exactCount = ExactFraudCount(vectors, labels, referenceCount, query);
    int nativeFileCount = ExactFraudCount(nativeFlat.Vectors, nativeFlat.Labels, nativeFlat.Count, query);
    int nativeCount = native.PredictFraudCount(payload);
    int expectedCount = ParseExpectedCount(entry.GetProperty("expected_fraud_score"));
    string id = request.GetProperty("id").GetString() ?? $"sample-{sample}";

    bool exactMismatch = exactCount != expectedCount;
    bool nativeFileMismatch = nativeFileCount != exactCount;
    bool nativeExactMismatch = nativeCount != exactCount;
    bool nativeExpectedMismatch = nativeCount != expectedCount;

    if (exactMismatch) exactVsExpectedMismatches++;
    if (nativeFileMismatch) nativeFileVsExactMismatches++;
    if (nativeExactMismatch) nativeVsExactMismatches++;
    if (nativeExpectedMismatch) nativeVsExpectedMismatches++;

    if (exactMismatch || nativeFileMismatch || nativeExactMismatch)
    {
        Console.WriteLine($"{id}: expected={expectedCount} exact={exactCount} native-file={nativeFileCount} native-tree={nativeCount}");
    }
}

Console.WriteLine();
Console.WriteLine($"exact vs expected mismatches: {exactVsExpectedMismatches}/{sampleCount}");
Console.WriteLine($"native file vs exact mismatches:{nativeFileVsExactMismatches}/{sampleCount}");
Console.WriteLine($"native vs exact mismatches:   {nativeVsExactMismatches}/{sampleCount}");
Console.WriteLine($"native vs expected mismatches:{nativeVsExpectedMismatches}/{sampleCount}");
return 0;

static async Task<byte[]> ReadInputAsync(string path)
{
    if (path.EndsWith(".gz", StringComparison.OrdinalIgnoreCase))
    {
        await using var source = File.OpenRead(path);
        await using var gzip = new GZipStream(source, CompressionMode.Decompress);
        await using var memory = new MemoryStream();
        await gzip.CopyToAsync(memory);
        return memory.ToArray();
    }

    return await File.ReadAllBytesAsync(path);
}

static int ParseExpectedCount(JsonElement element)
{
    double value = element.GetDouble();
    return (int)Math.Round(value * 5.0, MidpointRounding.AwayFromZero);
}

static NativeFlatIndex LoadNativeFlat(byte[] bytes)
{
    ReadOnlySpan<byte> magicTree = "RNATIDX1"u8;
    ReadOnlySpan<byte> magicTree2 = "RNATIDX2"u8;
    ReadOnlySpan<byte> magicIvf = "RIVF1\0\0\0"u8;
    ReadOnlySpan<byte> magicIvf2 = "RIVF2\0\0\0"u8;
    ReadOnlySpan<byte> span = bytes;

    if (span.Length >= magicIvf2.Length && span[..magicIvf2.Length].SequenceEqual(magicIvf2))
        return LoadNativeIvfAsFlat(span, hasClusterCounts: true);
    
    if (span.Length >= magicIvf.Length && span[..magicIvf.Length].SequenceEqual(magicIvf))
        return LoadNativeIvfAsFlat(span, hasClusterCounts: false);

    if (span.Length >= magicTree2.Length && span[..magicTree2.Length].SequenceEqual(magicTree2))
        return LoadNativeTree2AsFlat(span);

    if (span.Length < magicTree.Length || !span[..magicTree.Length].SequenceEqual(magicTree))
        throw new InvalidDataException("Invalid native index magic.");

    int cursor = magicTree.Length;
    int scale = ReadInt(span, ref cursor);
    int packedDimensions = ReadInt(span, ref cursor);
    int count = ReadInt(span, ref cursor);
    _ = ReadInt(span, ref cursor);
    int partitionCount = ReadInt(span, ref cursor);
    int nodeCount = ReadInt(span, ref cursor);

    if (scale != VectorSpec.FlatScale || packedDimensions != VectorSpec.PackedDimensions)
        throw new InvalidDataException("Incompatible native index format.");

    int headerSize = sizeof(int) * 4 + sizeof(short) * VectorSpec.PackedDimensions * 2;
    cursor += partitionCount * headerSize;
    cursor += nodeCount * headerSize;

    short[] vectors = new short[count * VectorSpec.PackedDimensions];
    for (int i = 0; i < vectors.Length; i++)
        vectors[i] = ReadShort(span, ref cursor);

    byte[] labels = new byte[count];
    span[cursor..(cursor + count)].CopyTo(labels);
    return new NativeFlatIndex(vectors, labels, count);
}

static NativeFlatIndex LoadNativeTree2AsFlat(ReadOnlySpan<byte> span)
{
    int cursor = 8;
    int scale = ReadInt(span, ref cursor);
    int packedDimensions = ReadInt(span, ref cursor);
    int count = ReadInt(span, ref cursor);
    int leafSize = ReadInt(span, ref cursor);
    int partitionCount = ReadInt(span, ref cursor);
    int nodeCount = ReadInt(span, ref cursor);
    int totalBlocks = ReadInt(span, ref cursor);

    int headerSize = sizeof(int) * 4 + sizeof(short) * VectorSpec.PackedDimensions * 2;
    cursor += partitionCount * headerSize;
    cursor += nodeCount * headerSize;

    int blocksSize = totalBlocks * VectorSpec.Dimensions * 8 * 2;
    ReadOnlySpan<byte> orderedBlocks = span.Slice(cursor, blocksSize);
    cursor += blocksSize;

    int labelsSize = totalBlocks * 8;
    ReadOnlySpan<byte> orderedLabels = span.Slice(cursor, labelsSize);
    cursor += labelsSize;

    short[] flatVectors = new short[totalBlocks * 8 * VectorSpec.PackedDimensions];
    byte[] flatLabels = new byte[totalBlocks * 8];

    int writeIdx = 0;
    for (int b = 0; b < totalBlocks; b++)
    {
        for (int lane = 0; lane < 8; lane++)
        {
            byte label = orderedLabels[b * 8 + lane];
            int vectorBase = writeIdx * VectorSpec.PackedDimensions;
            for (int d = 0; d < VectorSpec.Dimensions; d++)
            {
                int blockValOffset = (b * VectorSpec.Dimensions * 8 + d * 8 + lane) * 2;
                flatVectors[vectorBase + d] = BinaryPrimitives.ReadInt16LittleEndian(orderedBlocks.Slice(blockValOffset, 2));
            }
            flatLabels[writeIdx] = label;
            writeIdx++;
        }
    }

    return new NativeFlatIndex(flatVectors, flatLabels, writeIdx);
}

static NativeFlatIndex LoadNativeIvfAsFlat(ReadOnlySpan<byte> span, bool hasClusterCounts)
{
    int cursor = 8;
    int scale = ReadInt(span, ref cursor);
    int packedDimensions = ReadInt(span, ref cursor);
    int count = ReadInt(span, ref cursor);
    int centerCount = ReadInt(span, ref cursor);

    cursor += centerCount * VectorSpec.Dimensions * 2; // skip centers

    int[] blockOffsets = new int[centerCount + 1];
    for (int i = 0; i <= centerCount; i++)
    {
        blockOffsets[i] = ReadInt(span, ref cursor);
    }
    int totalBlocks = blockOffsets[centerCount];

    int[] clusterCounts = new int[centerCount];
    if (hasClusterCounts)
    {
        for (int i = 0; i < centerCount; i++)
            clusterCounts[i] = ReadInt(span, ref cursor);
    }
    else
    {
        for (int i = 0; i < centerCount; i++)
            clusterCounts[i] = (blockOffsets[i + 1] - blockOffsets[i]) * 8;
    }

    int labelsSize = totalBlocks * 8;
    ReadOnlySpan<byte> orderedLabels = span.Slice(cursor, labelsSize);
    cursor += labelsSize;

    ReadOnlySpan<byte> orderedBlocks = span.Slice(cursor, totalBlocks * VectorSpec.Dimensions * 8 * 2);
    
    short[] flatVectors = new short[count * VectorSpec.PackedDimensions];
    byte[] flatLabels = new byte[count];

    int writeIdx = 0;
    for (int c = 0; c < centerCount; c++)
    {
        int startBlock = blockOffsets[c];
        int clusterCount = clusterCounts[c];
        for (int item = 0; item < clusterCount; item++)
        {
            int block = startBlock + item / 8;
            int lane = item % 8;
            int vectorBase = writeIdx * VectorSpec.PackedDimensions;
            for (int d = 0; d < VectorSpec.Dimensions; d++)
            {
                int blockValOffset = (block * VectorSpec.Dimensions * 8 + d * 8 + lane) * 2;
                flatVectors[vectorBase + d] = BinaryPrimitives.ReadInt16LittleEndian(orderedBlocks.Slice(blockValOffset, 2));
            }
            flatLabels[writeIdx] = orderedLabels[block * 8 + lane];
            writeIdx++;
        }
    }

    if (writeIdx != count)
        throw new InvalidDataException($"IVF vector count mismatch: expected {count}, read {writeIdx}.");

    return new NativeFlatIndex(flatVectors, flatLabels, writeIdx);
}

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

    int newCapacity = labels.Length * 2;
    Array.Resize(ref labels, newCapacity);
    Array.Resize(ref vectors, newCapacity * VectorSpec.PackedDimensions);
}

static int ReadInt(ReadOnlySpan<byte> span, ref int cursor)
{
    int value = BinaryPrimitives.ReadInt32LittleEndian(span.Slice(cursor, sizeof(int)));
    cursor += sizeof(int);
    return value;
}

static short ReadShort(ReadOnlySpan<byte> span, ref int cursor)
{
    short value = BinaryPrimitives.ReadInt16LittleEndian(span.Slice(cursor, sizeof(short)));
    cursor += sizeof(short);
    return value;
}

static int ExactFraudCount(short[] vectors, byte[] labels, int count, ReadOnlySpan<short> query)
{
    Span<long> bestDistances = stackalloc long[5];
    Span<byte> bestLabels = stackalloc byte[5];
    bestDistances.Fill(long.MaxValue);

    for (int i = 0; i < count; i++)
    {
        int vectorBase = i * VectorSpec.PackedDimensions;
        long distance = 0;
        for (int d = 0; d < VectorSpec.PackedDimensions; d++)
        {
            long diff = query[d] - vectors[vectorBase + d];
            distance += diff * diff;
        }

        if (distance >= bestDistances[4])
            continue;

        InsertBest(distance, labels[i], bestDistances, bestLabels);
    }

    int frauds = 0;
    for (int i = 0; i < bestLabels.Length; i++)
        frauds += bestLabels[i];
    return frauds;
}

static void InsertBest(long distance, byte label, Span<long> bestDistances, Span<byte> bestLabels)
{
    int position = bestDistances.Length - 1;
    while (position > 0 && distance < bestDistances[position - 1])
    {
        bestDistances[position] = bestDistances[position - 1];
        bestLabels[position] = bestLabels[position - 1];
        position--;
    }

    bestDistances[position] = distance;
    bestLabels[position] = label;
}

internal sealed record NativeFlatIndex(short[] Vectors, byte[] Labels, int Count);
