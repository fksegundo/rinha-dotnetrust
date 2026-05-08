using System.Buffers.Binary;
using System.IO.Compression;
using System.Text;
using System.Text.Json;
using Rinha.Core;

if (args.Length < 3)
{
    Console.Error.WriteLine("Usage: Rinha.VerifyNative <references.json|references.json.gz> <test-data.json> <native.idx> [sampleCount|all] [--stop-on-first-mismatch]");
    return 2;
}

string referencesPath = args[0];
string testDataPath = args[1];
string nativeIndexPath = args[2];
bool stopOnFirstMismatch = args.Skip(3).Any(static arg => arg.Equals("--stop-on-first-mismatch", StringComparison.OrdinalIgnoreCase));
bool verifyAll = args.Length > 3 && args[3].Equals("all", StringComparison.OrdinalIgnoreCase);
int sampleCount = verifyAll
    ? int.MaxValue
    : args.Length > 3 && !args[3].StartsWith("--", StringComparison.Ordinal)
        ? int.Parse(args[3])
        : 32;

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
sampleCount = verifyAll ? allEntries.Length : Math.Clamp(sampleCount, 1, allEntries.Length);

using var native = NativeFraudSearch.Open(nativeIndexPath);

int exactVsExpectedMismatches = 0;
int nativeFileVsExactMismatches = 0;
int nativeVsExactMismatches = 0;
int nativeVsExpectedMismatches = 0;

Console.WriteLine($"Verifying {sampleCount} sampled requests");
Span<short> query = stackalloc short[VectorSpec.PackedDimensions];
int processed = 0;
for (int sample = 0; sample < sampleCount; sample++)
{
    int entryIndex = verifyAll ? sample : sample * allEntries.Length / sampleCount;
    JsonElement entry = allEntries[entryIndex];
    JsonElement request = entry.GetProperty("request");
    string requestJson = request.GetRawText();
    byte[] payload = Encoding.UTF8.GetBytes(requestJson);

    if (!FraudVectorizer.TryVectorizeFlat(payload, query))
    {
        Console.WriteLine($"[{sample + 1}/{sampleCount}] vectorization failed for {entry.GetProperty("request").GetProperty("id").GetString()}");
        continue;
    }

    int nativeCount = native.PredictFraudCount(payload);
    int expectedCount = ParseExpectedCount(entry.GetProperty("expected_fraud_score"));
    bool expectedApproved = entry.GetProperty("expected_approved").GetBoolean();
    bool nativeApproved = nativeCount < 3;
    string id = request.GetProperty("id").GetString() ?? $"sample-{sample}";

    bool nativeExpectedMismatch = nativeCount != expectedCount;
    bool exactMismatch = false;
    bool nativeFileMismatch = false;
    bool nativeExactMismatch = false;
    int exactCount = -1;
    int nativeFileCount = -1;

    if (nativeExpectedMismatch) nativeVsExpectedMismatches++;

    if (nativeExpectedMismatch || !verifyAll)
    {
        exactCount = ExactFraudCount(vectors, labels, referenceCount, query);
        nativeFileCount = ExactFraudCount(nativeFlat.Vectors, nativeFlat.Labels, nativeFlat.Count, query);
        exactMismatch = exactCount != expectedCount;
        nativeFileMismatch = nativeFileCount != exactCount;
        nativeExactMismatch = nativeCount != exactCount;

        if (exactMismatch) exactVsExpectedMismatches++;
        if (nativeFileMismatch) nativeFileVsExactMismatches++;
        if (nativeExactMismatch) nativeVsExactMismatches++;
    }

    if (nativeExpectedMismatch)
    {
        PrintMismatchDetails(
            id,
            expectedCount,
            exactCount,
            nativeFileCount,
            nativeCount,
            expectedApproved,
            nativeApproved,
            query,
            payload,
            referencesJson,
            vectors,
            labels,
            referenceCount);

        processed++;
        if (stopOnFirstMismatch)
            break;
    }
    else if (!verifyAll && (exactMismatch || nativeFileMismatch || nativeExactMismatch))
    {
        Console.WriteLine($"{id}: expected={expectedCount} exact={exactCount} native-file={nativeFileCount} native-tree={nativeCount}");
    }

    if (!nativeExpectedMismatch)
        processed++;
    if (verifyAll && processed % 5_000 == 0)
    {
        Console.WriteLine($"Processed {processed:n0}/{sampleCount:n0} requests, native vs expected mismatches: {nativeVsExpectedMismatches}");
    }
}

Console.WriteLine();
Console.WriteLine($"exact vs expected mismatches: {exactVsExpectedMismatches}/{processed}");
Console.WriteLine($"native file vs exact mismatches:{nativeFileVsExactMismatches}/{processed}");
Console.WriteLine($"native vs exact mismatches:   {nativeVsExactMismatches}/{processed}");
Console.WriteLine($"native vs expected mismatches:{nativeVsExpectedMismatches}/{processed}");
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
    ExactTopK topK = ExactTopKSearch(vectors, labels, count, query, 5);
    ReadOnlySpan<byte> bestLabels = topK.Labels;
    int frauds = 0;
    for (int i = 0; i < bestLabels.Length; i++)
        frauds += bestLabels[i];
    return frauds;
}

static ExactTopK ExactTopKSearch(short[] vectors, byte[] labels, int count, ReadOnlySpan<short> query, int k)
{
    long[] bestDistances = new long[k];
    byte[] bestLabels = new byte[k];
    int[] bestIndices = new int[k];
    Array.Fill(bestDistances, long.MaxValue);
    Array.Fill(bestIndices, -1);

    for (int i = 0; i < count; i++)
    {
        int vectorBase = i * VectorSpec.PackedDimensions;
        long distance = 0;
        for (int d = 0; d < VectorSpec.PackedDimensions; d++)
        {
            long diff = query[d] - vectors[vectorBase + d];
            distance += diff * diff;
        }

        if (distance >= bestDistances[k - 1])
            continue;

        InsertBest(distance, labels[i], i, bestDistances, bestLabels, bestIndices);
    }

    return new ExactTopK(bestDistances, bestLabels, bestIndices);
}

static void InsertBest(long distance, byte label, int index, long[] bestDistances, byte[] bestLabels, int[] bestIndices)
{
    int position = bestDistances.Length - 1;
    while (position > 0 && distance < bestDistances[position - 1])
    {
        bestDistances[position] = bestDistances[position - 1];
        bestLabels[position] = bestLabels[position - 1];
        bestIndices[position] = bestIndices[position - 1];
        position--;
    }

    bestDistances[position] = distance;
    bestLabels[position] = label;
    bestIndices[position] = index;
}

static void PrintMismatchDetails(
    string id,
    int expectedCount,
    int exactCount,
    int nativeFileCount,
    int nativeCount,
    bool expectedApproved,
    bool nativeApproved,
    ReadOnlySpan<short> query,
    ReadOnlySpan<byte> payload,
    byte[] referencesJson,
    short[] vectors,
    byte[] labels,
    int referenceCount)
{
    string mismatchType = expectedApproved && !nativeApproved ? "false_positive" : "false_negative";
    ExactTopK exactTop5 = ExactTopKSearch(vectors, labels, referenceCount, query, 5);
    double[] rawQuery = VectorizeRaw(payload);
    FloatTopK? rawTop5 = rawQuery.Length > 0 ? ExactFloatTopKSearch(referencesJson, rawQuery, 5) : null;

    Console.WriteLine($"Mismatch for {id}");
    Console.WriteLine($"  type={mismatchType} expected={expectedCount} approved={expectedApproved} native-tree={nativeCount} approved={nativeApproved} exact={exactCount} native-file={nativeFileCount}");
    Console.WriteLine($"  query=[{string.Join(", ", query.ToArray())}]");
    if (rawTop5 is not null)
    {
        int rawFrauds = rawTop5.Labels.Sum(static label => label);
        Console.WriteLine($"  raw-float-exact={rawFrauds}");
        Console.WriteLine($"  raw-query=[{string.Join(", ", rawQuery.Select(static value => value.ToString("0.########")))}]");
    }
    Console.WriteLine("  exact-top5:");
    for (int i = 0; i < exactTop5.Indices.Length; i++)
    {
        if (exactTop5.Indices[i] < 0)
            continue;

        Console.WriteLine($"    #{i + 1}: index={exactTop5.Indices[i]} label={exactTop5.Labels[i]} distance={exactTop5.Distances[i]}");
    }

    if (rawTop5 is not null)
    {
        Console.WriteLine("  raw-top5:");
        for (int i = 0; i < rawTop5.Indices.Length; i++)
        {
            Console.WriteLine($"    #{i + 1}: index={rawTop5.Indices[i]} label={rawTop5.Labels[i]} distance={rawTop5.Distances[i]:0.########}");
        }
    }
}

static double[] VectorizeRaw(ReadOnlySpan<byte> json)
{
    double[] destination = new double[VectorSpec.PackedDimensions];
    double amount = 0;
    double customerAvgAmount = 1;
    long requestedMinute = 0;
    long lastMinute = 0;
    bool hasLastTransaction = false;
    bool inKnownMerchants = false;
    ulong merchantHash = 0;
    Span<ulong> knownHashes = stackalloc ulong[64];
    int knownCount = 0;

    Span<RawContext> contexts = stackalloc RawContext[16];
    RawContext pendingContext = RawContext.None;
    RawField pendingField = RawField.None;

    var reader = new Utf8JsonReader(json, isFinalBlock: true, state: default);

    while (reader.Read())
    {
        switch (reader.TokenType)
        {
            case JsonTokenType.StartObject:
                if (pendingContext != RawContext.None && reader.CurrentDepth < contexts.Length)
                {
                    contexts[reader.CurrentDepth] = pendingContext;
                    if (pendingContext == RawContext.LastTransaction)
                        hasLastTransaction = true;
                    pendingContext = RawContext.None;
                }
                break;

            case JsonTokenType.EndObject:
                if (reader.CurrentDepth < contexts.Length)
                    contexts[reader.CurrentDepth] = RawContext.None;
                break;

            case JsonTokenType.StartArray:
                if (pendingField == RawField.KnownMerchants)
                {
                    inKnownMerchants = true;
                    pendingField = RawField.None;
                }
                break;

            case JsonTokenType.EndArray:
                inKnownMerchants = false;
                break;

            case JsonTokenType.PropertyName:
                if (reader.CurrentDepth == 1)
                {
                    pendingContext = RootRawContext(reader.ValueSpan);
                    pendingField = RawField.None;
                    break;
                }

                var context = reader.CurrentDepth > 0 && reader.CurrentDepth - 1 < contexts.Length
                    ? contexts[reader.CurrentDepth - 1]
                    : RawContext.None;
                pendingField = ResolveRawField(context, reader.ValueSpan);
                break;

            case JsonTokenType.Null:
                if (pendingContext == RawContext.LastTransaction)
                    pendingContext = RawContext.None;
                pendingField = RawField.None;
                break;

            case JsonTokenType.String:
                if (inKnownMerchants)
                {
                    if (knownCount < knownHashes.Length)
                        knownHashes[knownCount++] = Hash(reader.ValueSpan);
                    break;
                }

                switch (pendingField)
                {
                    case RawField.RequestedAt:
                        requestedMinute = ParseEpochMinute(reader.ValueSpan);
                        destination[3] = Parse2(reader.ValueSpan, 11) / 23.0;
                        destination[4] = DayOfWeekMondayZero(reader.ValueSpan) / 6.0;
                        break;
                    case RawField.MerchantId:
                        merchantHash = Hash(reader.ValueSpan);
                        break;
                    case RawField.MerchantMcc:
                        destination[12] = MccRisk(reader.ValueSpan);
                        break;
                    case RawField.LastTimestamp:
                        lastMinute = ParseEpochMinute(reader.ValueSpan);
                        break;
                }
                pendingField = RawField.None;
                break;

            case JsonTokenType.Number:
                switch (pendingField)
                {
                    case RawField.Amount:
                        amount = reader.GetDouble();
                        destination[0] = Math.Clamp(amount / 10_000.0, 0, 1);
                        break;
                    case RawField.Installments:
                        destination[1] = Math.Clamp(reader.GetInt32() / 12.0, 0, 1);
                        break;
                    case RawField.CustomerAvgAmount:
                        customerAvgAmount = reader.GetDouble();
                        break;
                    case RawField.TxCount24h:
                        destination[8] = Math.Clamp(reader.GetInt32() / 20.0, 0, 1);
                        break;
                    case RawField.MerchantAvgAmount:
                        destination[13] = Math.Clamp(reader.GetDouble() / 10_000.0, 0, 1);
                        break;
                    case RawField.KmFromHome:
                        destination[7] = Math.Clamp(reader.GetDouble() / 1_000.0, 0, 1);
                        break;
                    case RawField.LastKm:
                        destination[6] = Math.Clamp(reader.GetDouble() / 1_000.0, 0, 1);
                        break;
                }
                pendingField = RawField.None;
                break;

            case JsonTokenType.True:
            case JsonTokenType.False:
                double bit = reader.TokenType == JsonTokenType.True ? 1.0 : 0.0;
                if (pendingField == RawField.IsOnline)
                    destination[9] = bit;
                else if (pendingField == RawField.CardPresent)
                    destination[10] = bit;
                pendingField = RawField.None;
                break;
        }
    }

    destination[2] = customerAvgAmount > 0
        ? Math.Clamp((amount / customerAvgAmount) / 10.0, 0, 1)
        : 1.0;

    if (hasLastTransaction)
        destination[5] = Math.Clamp(Math.Max(0, requestedMinute - lastMinute) / 1_440.0, 0, 1);
    else
    {
        destination[5] = -1.0;
        destination[6] = -1.0;
    }

    bool knownMerchant = false;
    for (int i = 0; i < knownCount; i++)
    {
        if (knownHashes[i] == merchantHash)
        {
            knownMerchant = true;
            break;
        }
    }
    destination[11] = knownMerchant ? 0.0 : 1.0;
    return destination;
}

static FloatTopK ExactFloatTopKSearch(byte[] referencesJson, double[] query, int k)
{
    double[] bestDistances = new double[k];
    byte[] bestLabels = new byte[k];
    int[] bestIndices = new int[k];
    Array.Fill(bestDistances, double.MaxValue);
    Array.Fill(bestIndices, -1);

    var reader = new Utf8JsonReader(referencesJson.AsSpan(), isFinalBlock: true, state: default);
    Span<double> currentVector = stackalloc double[VectorSpec.PackedDimensions];
    int vectorPosition = -1;
    bool inVector = false;
    bool expectingLabel = false;
    int currentIndex = 0;

    while (reader.Read())
    {
        switch (reader.TokenType)
        {
            case JsonTokenType.PropertyName:
                inVector = false;
                expectingLabel = false;
                if (reader.ValueSpan.SequenceEqual("vector"u8))
                {
                    currentVector.Clear();
                    vectorPosition = 0;
                }
                else if (reader.ValueSpan.SequenceEqual("label"u8))
                {
                    expectingLabel = true;
                }
                break;

            case JsonTokenType.StartArray when vectorPosition == 0:
                inVector = true;
                break;

            case JsonTokenType.Number when inVector:
                if (vectorPosition < VectorSpec.Dimensions)
                    currentVector[vectorPosition] = reader.GetDouble();
                vectorPosition++;
                break;

            case JsonTokenType.EndArray when inVector:
                inVector = false;
                break;

            case JsonTokenType.String when expectingLabel:
                byte label = reader.ValueSpan.SequenceEqual("fraud"u8) ? (byte)1 : (byte)0;
                double distance = 0;
                for (int d = 0; d < VectorSpec.Dimensions; d++)
                {
                    double diff = query[d] - currentVector[d];
                    distance += diff * diff;
                }

                if (distance < bestDistances[k - 1])
                    InsertBestFloat(distance, label, currentIndex, bestDistances, bestLabels, bestIndices);

                expectingLabel = false;
                currentIndex++;
                break;
        }
    }

    return new FloatTopK(bestDistances, bestLabels, bestIndices);
}

static void InsertBestFloat(double distance, byte label, int index, double[] bestDistances, byte[] bestLabels, int[] bestIndices)
{
    int position = bestDistances.Length - 1;
    while (position > 0 && distance < bestDistances[position - 1])
    {
        bestDistances[position] = bestDistances[position - 1];
        bestLabels[position] = bestLabels[position - 1];
        bestIndices[position] = bestIndices[position - 1];
        position--;
    }

    bestDistances[position] = distance;
    bestLabels[position] = label;
    bestIndices[position] = index;
}

static RawContext RootRawContext(ReadOnlySpan<byte> name)
{
    if (name.SequenceEqual("transaction"u8)) return RawContext.Transaction;
    if (name.SequenceEqual("customer"u8)) return RawContext.Customer;
    if (name.SequenceEqual("merchant"u8)) return RawContext.Merchant;
    if (name.SequenceEqual("terminal"u8)) return RawContext.Terminal;
    if (name.SequenceEqual("last_transaction"u8)) return RawContext.LastTransaction;
    return RawContext.None;
}

static RawField ResolveRawField(RawContext context, ReadOnlySpan<byte> name)
{
    return context switch
    {
        RawContext.Transaction when name.SequenceEqual("amount"u8) => RawField.Amount,
        RawContext.Transaction when name.SequenceEqual("installments"u8) => RawField.Installments,
        RawContext.Transaction when name.SequenceEqual("requested_at"u8) => RawField.RequestedAt,
        RawContext.Customer when name.SequenceEqual("avg_amount"u8) => RawField.CustomerAvgAmount,
        RawContext.Customer when name.SequenceEqual("tx_count_24h"u8) => RawField.TxCount24h,
        RawContext.Customer when name.SequenceEqual("known_merchants"u8) => RawField.KnownMerchants,
        RawContext.Merchant when name.SequenceEqual("id"u8) => RawField.MerchantId,
        RawContext.Merchant when name.SequenceEqual("mcc"u8) => RawField.MerchantMcc,
        RawContext.Merchant when name.SequenceEqual("avg_amount"u8) => RawField.MerchantAvgAmount,
        RawContext.Terminal when name.SequenceEqual("is_online"u8) => RawField.IsOnline,
        RawContext.Terminal when name.SequenceEqual("card_present"u8) => RawField.CardPresent,
        RawContext.Terminal when name.SequenceEqual("km_from_home"u8) => RawField.KmFromHome,
        RawContext.LastTransaction when name.SequenceEqual("timestamp"u8) => RawField.LastTimestamp,
        RawContext.LastTransaction when name.SequenceEqual("km_from_current"u8) => RawField.LastKm,
        _ => RawField.None
    };
}

static ulong Hash(ReadOnlySpan<byte> value)
{
    ulong hash = 14_695_981_039_346_656_037UL;
    for (int i = 0; i < value.Length; i++)
    {
        hash ^= value[i];
        hash *= 1_099_511_628_211UL;
    }
    return hash;
}

static double MccRisk(ReadOnlySpan<byte> mcc)
{
    if (mcc.SequenceEqual("5411"u8)) return 0.15;
    if (mcc.SequenceEqual("5812"u8)) return 0.30;
    if (mcc.SequenceEqual("5912"u8)) return 0.20;
    if (mcc.SequenceEqual("5944"u8)) return 0.45;
    if (mcc.SequenceEqual("7801"u8)) return 0.80;
    if (mcc.SequenceEqual("7802"u8)) return 0.75;
    if (mcc.SequenceEqual("7995"u8)) return 0.85;
    if (mcc.SequenceEqual("4511"u8)) return 0.35;
    if (mcc.SequenceEqual("5311"u8)) return 0.25;
    if (mcc.SequenceEqual("5999"u8)) return 0.50;
    return 0.50;
}

static int Parse2(ReadOnlySpan<byte> s, int offset) => ((s[offset] - '0') * 10) + (s[offset + 1] - '0');
static int Parse4(ReadOnlySpan<byte> s, int offset) => (Parse2(s, offset) * 100) + Parse2(s, offset + 2);

static long ParseEpochMinute(ReadOnlySpan<byte> iso)
{
    int y = Parse4(iso, 0);
    int m = Parse2(iso, 5);
    int d = Parse2(iso, 8);
    int hh = Parse2(iso, 11);
    int mm = Parse2(iso, 14);
    return DaysFromCivil(y, m, d) * 1_440L + hh * 60L + mm;
}

static int DayOfWeekMondayZero(ReadOnlySpan<byte> iso)
{
    int y = Parse4(iso, 0);
    int m = Parse2(iso, 5);
    int d = Parse2(iso, 8);
    long days = DaysFromCivil(y, m, d);
    return (int)((days + 3) % 7);
}

static long DaysFromCivil(int y, int m, int d)
{
    y -= m <= 2 ? 1 : 0;
    int era = (y >= 0 ? y : y - 399) / 400;
    uint yoe = (uint)(y - era * 400);
    uint doy = (uint)((153 * (m + (m > 2 ? -3 : 9)) + 2) / 5 + d - 1);
    uint doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    return era * 146_097L + doe - 719_468;
}

internal sealed record NativeFlatIndex(short[] Vectors, byte[] Labels, int Count);
internal sealed record ExactTopK(long[] Distances, byte[] Labels, int[] Indices);
internal sealed record FloatTopK(double[] Distances, byte[] Labels, int[] Indices);

enum RawContext : byte
{
    None,
    Transaction,
    Customer,
    Merchant,
    Terminal,
    LastTransaction
}

enum RawField : byte
{
    None,
    Amount,
    Installments,
    RequestedAt,
    CustomerAvgAmount,
    TxCount24h,
    KnownMerchants,
    MerchantId,
    MerchantMcc,
    MerchantAvgAmount,
    IsOnline,
    CardPresent,
    KmFromHome,
    LastTimestamp,
    LastKm
}
