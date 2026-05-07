using System.IO.Compression;
using System.Text.Json;
using Rinha.Core;

if (args.Length < 2)
{
    Console.Error.WriteLine("Usage: Rinha.Preprocess <references.json.gz> <output.idx> [native <leafSize>|ivf <centers> <sampleSize> <iterations>|flat <maxReferences>|centers sampleSize iterations]");
    return 2;
}

string inputPath = args[0];
string outputPath = args[1];
bool ivfMode = args.Length > 2 && string.Equals(args[2], "ivf", StringComparison.OrdinalIgnoreCase);
bool nativeMode = args.Length > 2 && string.Equals(args[2], "native", StringComparison.OrdinalIgnoreCase);
bool flatMode = args.Length > 2 && string.Equals(args[2], "flat", StringComparison.OrdinalIgnoreCase);
int centerCount = VectorSpec.DefaultCenters;
int sampleSize = VectorSpec.DefaultSampleSize;
int iterations = VectorSpec.DefaultIterations;

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
if (nativeMode)
{
    int leafSize = args.Length > 3 ? int.Parse(args[3]) : 256;
    var (flatVectors, flatLabels, flatCount) = ParseFlatReferences(json);
    Console.WriteLine($"Parsed {flatCount:n0} references");
    Console.WriteLine($"Building native exact tree index with leaf size {leafSize}");
    NativeIndexWriter.Write(outputPath, flatVectors, flatLabels, flatCount, leafSize);
    Console.WriteLine("Done");
    return 0;
}

if (ivfMode)
{
    int k = args.Length > 3 ? int.Parse(args[3]) : 4096;
    int kSampleSize = args.Length > 4 ? int.Parse(args[4]) : 30_000;
    int kIterations = args.Length > 5 ? int.Parse(args[5]) : 10;
    var (flatVectors, flatLabels, flatCount) = ParseFlatReferences(json);
    Console.WriteLine($"Parsed {flatCount:n0} references");
    Console.WriteLine($"Building native IVF index with {k} centers");
    Rinha.Preprocess.NativeIvfIndexWriter.Write(outputPath, flatVectors, flatLabels, flatCount, k, kSampleSize, kIterations);
    Console.WriteLine("Done");
    return 0;
}

if (flatMode)
{
    int maxReferences = args.Length > 3 ? int.Parse(args[3]) : 12_000;
    var (flatVectors, flatLabels, flatCount) = ParseFlatReferences(json);
    Console.WriteLine($"Parsed {flatCount:n0} references");

    Console.WriteLine($"Selecting {maxReferences:n0} stratified flat references");
    var (selectedVectors, selectedLabels, selectedCount) = SelectStratified(flatVectors, flatLabels, flatCount, maxReferences);

    Console.WriteLine($"Writing {selectedCount:n0} flat references to {outputPath}");
    FraudIndex.WriteFlat(outputPath, selectedVectors, selectedLabels);
    Console.WriteLine("Done");
    return 0;
}

var (vectors, labels, count) = ParseReferences(json);
Console.WriteLine($"Parsed {count:n0} references");

centerCount = args.Length > 2 ? int.Parse(args[2]) : VectorSpec.DefaultCenters;
sampleSize = args.Length > 3 ? int.Parse(args[3]) : VectorSpec.DefaultSampleSize;
iterations = args.Length > 4 ? int.Parse(args[4]) : VectorSpec.DefaultIterations;
centerCount = Math.Min(centerCount, count);
sampleSize = Math.Min(sampleSize, count);

int[] assignments = new int[count];
int[] offsets = new int[centerCount + 1];

sbyte[] centers;
if (centerCount == VectorSpec.DefaultCenters && Environment.GetEnvironmentVariable("RINHA_TRAIN_KMEANS") != "1")
{
    Console.WriteLine("Building 512-bucket ANN index");
    centers = BuildBucketAssignments(vectors, count, assignments, offsets);
}
else
{
    Console.WriteLine($"Training {centerCount} centers from {sampleSize:n0} samples for {iterations} iterations");
    centers = TrainCenters(vectors, count, centerCount, sampleSize, iterations);

    Console.WriteLine("Assigning references to centers");
    for (int i = 0; i < count; i++)
    {
        int center = NearestCenter(vectors.AsSpan(i * VectorSpec.PackedDimensions, VectorSpec.PackedDimensions), centers, centerCount);
        assignments[i] = center;
        offsets[center + 1]++;
    }
}

for (int i = 1; i < offsets.Length; i++)
    offsets[i] += offsets[i - 1];

int[] next = new int[centerCount];
Array.Copy(offsets, next, centerCount);
var orderedVectors = new sbyte[count * VectorSpec.PackedDimensions];
var orderedLabels = new byte[count];

for (int i = 0; i < count; i++)
{
    int position = next[assignments[i]]++;
    vectors.AsSpan(i * VectorSpec.PackedDimensions, VectorSpec.PackedDimensions)
        .CopyTo(orderedVectors.AsSpan(position * VectorSpec.PackedDimensions, VectorSpec.PackedDimensions));
    orderedLabels[position] = labels[i];
}

Console.WriteLine($"Writing {outputPath}");
FraudIndex.Write(outputPath, centerCount, offsets, centers, orderedVectors, orderedLabels);
Console.WriteLine("Done");
return 0;

static (sbyte[] Vectors, byte[] Labels, int Count) ParseReferences(byte[] json)
{
    const int initialCapacity = 3_000_000;
    var vectors = new sbyte[initialCapacity * VectorSpec.PackedDimensions];
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
                EnsureCapacity(ref vectors, ref labels, count);
                if (vectorPosition < VectorSpec.Dimensions)
                    vectors[count * VectorSpec.PackedDimensions + vectorPosition] = VectorSpec.Quantize(reader.GetDouble());
                vectorPosition++;
                break;

            case JsonTokenType.EndArray when inVector:
                inVector = false;
                break;

            case JsonTokenType.String when expectingLabel:
                EnsureCapacity(ref vectors, ref labels, count);
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

static (short[] Vectors, byte[] Labels, int Count) SelectStratified(short[] vectors, byte[] labels, int count, int maxReferences)
{
    int target = Math.Min(count, Math.Max(5, maxReferences));
    int fraudCount = 0;
    for (int i = 0; i < count; i++)
        fraudCount += labels[i];

    int legitCount = count - fraudCount;
    int targetFraud = fraudCount == 0
        ? 0
        : Math.Clamp((int)Math.Round((double)target * fraudCount / count), Math.Min(5, fraudCount), fraudCount);
    int targetLegit = Math.Min(legitCount, target - targetFraud);
    target = targetFraud + targetLegit;

    var selectedVectors = new short[target * VectorSpec.PackedDimensions];
    var selectedLabels = new byte[target];

    int seenFraud = 0;
    int seenLegit = 0;
    int selectedFraud = 0;
    int selectedLegit = 0;
    int selected = 0;

    for (int i = 0; i < count && selected < target; i++)
    {
        byte label = labels[i];
        bool take;
        if (label == 1)
        {
            take = ShouldSelect(seenFraud, selectedFraud, fraudCount, targetFraud);
            seenFraud++;
            if (take)
                selectedFraud++;
        }
        else
        {
            take = ShouldSelect(seenLegit, selectedLegit, legitCount, targetLegit);
            seenLegit++;
            if (take)
                selectedLegit++;
        }

        if (!take)
            continue;

        vectors.AsSpan(i * VectorSpec.PackedDimensions, VectorSpec.PackedDimensions)
            .CopyTo(selectedVectors.AsSpan(selected * VectorSpec.PackedDimensions, VectorSpec.PackedDimensions));
        selectedLabels[selected] = label;
        selected++;
    }

    if (selected != target)
    {
        Array.Resize(ref selectedVectors, selected * VectorSpec.PackedDimensions);
        Array.Resize(ref selectedLabels, selected);
    }

    Console.WriteLine($"Selected {selectedFraud:n0} fraud + {selectedLegit:n0} legit references");
    return (selectedVectors, selectedLabels, selected);
}

static void EnsureCapacity(ref sbyte[] vectors, ref byte[] labels, int count)
{
    if (count < labels.Length)
        return;

    int newCapacity = labels.Length + labels.Length / 2;
    Array.Resize(ref labels, newCapacity);
    Array.Resize(ref vectors, newCapacity * VectorSpec.PackedDimensions);
}

static void EnsureFlatCapacity(ref short[] vectors, ref byte[] labels, int count)
{
    if (count < labels.Length)
        return;

    int newCapacity = labels.Length + labels.Length / 2;
    Array.Resize(ref labels, newCapacity);
    Array.Resize(ref vectors, newCapacity * VectorSpec.PackedDimensions);
}

static bool ShouldSelect(int seen, int selected, int total, int target)
{
    return target > 0 && selected < target && (long)seen * target / Math.Max(1, total) >= selected;
}

static sbyte[] TrainCenters(sbyte[] vectors, int count, int centerCount, int sampleSize, int iterations)
{
    var centers = new sbyte[centerCount * VectorSpec.PackedDimensions];
    int stride = Math.Max(1, count / sampleSize);
    for (int c = 0; c < centerCount; c++)
    {
        int source = Math.Min(count - 1, c * stride);
        vectors.AsSpan(source * VectorSpec.PackedDimensions, VectorSpec.PackedDimensions)
            .CopyTo(centers.AsSpan(c * VectorSpec.PackedDimensions, VectorSpec.PackedDimensions));
    }

    var sums = new int[centerCount * VectorSpec.PackedDimensions];
    var counts = new int[centerCount];

    for (int iteration = 0; iteration < iterations; iteration++)
    {
        Array.Clear(sums);
        Array.Clear(counts);
        for (int s = 0; s < sampleSize; s++)
        {
            int item = Math.Min(count - 1, s * stride);
            var vector = vectors.AsSpan(item * VectorSpec.PackedDimensions, VectorSpec.PackedDimensions);
            int center = NearestCenter(vector, centers, centerCount);
            counts[center]++;
            int sumBase = center * VectorSpec.PackedDimensions;
            for (int d = 0; d < VectorSpec.Dimensions; d++)
                sums[sumBase + d] += vector[d];
        }

        for (int c = 0; c < centerCount; c++)
        {
            if (counts[c] == 0)
                continue;
            int offset = c * VectorSpec.PackedDimensions;
            for (int d = 0; d < VectorSpec.Dimensions; d++)
                centers[offset + d] = (sbyte)Math.Clamp((int)Math.Round((double)sums[offset + d] / counts[c]), -VectorSpec.Scale, VectorSpec.Scale);
        }

        Console.WriteLine($"  iteration {iteration + 1}/{iterations}");
    }

    return centers;
}

static sbyte[] BuildBucketAssignments(sbyte[] vectors, int count, int[] assignments, int[] offsets)
{
    var centers = new sbyte[VectorSpec.DefaultCenters * VectorSpec.PackedDimensions];
    var sums = new int[VectorSpec.DefaultCenters * VectorSpec.PackedDimensions];
    var counts = new int[VectorSpec.DefaultCenters];

    for (int i = 0; i < count; i++)
    {
        var vector = vectors.AsSpan(i * VectorSpec.PackedDimensions, VectorSpec.PackedDimensions);
        int bucket = BucketFor(vector);
        assignments[i] = bucket;
        offsets[bucket + 1]++;
        counts[bucket]++;

        int sumBase = bucket * VectorSpec.PackedDimensions;
        for (int d = 0; d < VectorSpec.Dimensions; d++)
            sums[sumBase + d] += vector[d];
    }

    int firstNonEmpty = Array.FindIndex(counts, static x => x > 0);
    for (int c = 0; c < VectorSpec.DefaultCenters; c++)
    {
        int source = counts[c] > 0 ? c : firstNonEmpty;
        int centerBase = c * VectorSpec.PackedDimensions;
        int sumBase = source * VectorSpec.PackedDimensions;
        int divisor = Math.Max(1, counts[source]);
        for (int d = 0; d < VectorSpec.Dimensions; d++)
            centers[centerBase + d] = (sbyte)Math.Clamp((int)Math.Round((double)sums[sumBase + d] / divisor), -VectorSpec.Scale, VectorSpec.Scale);
    }

    return centers;
}

static int BucketFor(ReadOnlySpan<sbyte> vector)
{
    int bucket = 0;
    if (vector[2] > 50) bucket |= 1 << 0;  // high amount vs customer average
    if (vector[5] < 0) bucket |= 1 << 1;   // no last transaction
    if (vector[7] > 50) bucket |= 1 << 2;  // far from home
    if (vector[8] > 50) bucket |= 1 << 3;  // many transactions in 24h
    if (vector[9] > 0) bucket |= 1 << 4;   // online
    if (vector[10] == 0) bucket |= 1 << 5; // card not present
    if (vector[11] > 0) bucket |= 1 << 6;  // unknown merchant
    if (vector[12] > 50) bucket |= 1 << 7; // high-risk MCC
    if (vector[0] > 50) bucket |= 1 << 8;  // high absolute amount
    return bucket;
}

static int NearestCenter(ReadOnlySpan<sbyte> vector, sbyte[] centers, int centerCount)
{
    int best = 0;
    int bestDistance = int.MaxValue;
    for (int c = 0; c < centerCount; c++)
    {
        int distance = 0;
        int offset = c * VectorSpec.PackedDimensions;
        for (int d = 0; d < VectorSpec.Dimensions; d++)
        {
            int diff = vector[d] - centers[offset + d];
            distance += diff * diff;
            if (distance >= bestDistance)
                break;
        }

        if (distance < bestDistance)
        {
            bestDistance = distance;
            best = c;
        }
    }
    return best;
}
