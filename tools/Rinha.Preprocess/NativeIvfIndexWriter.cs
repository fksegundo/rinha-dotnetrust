using System.Buffers.Binary;
using System.Runtime.InteropServices;
using Rinha.Core;

namespace Rinha.Preprocess;

internal static class NativeIvfIndexWriter
{
    private static readonly byte[] Magic = "RIVF2\0\0\0"u8.ToArray();
    private const int Lanes = 8;

    public static void Write(string path, short[] vectors, byte[] labels, int count, int centerCount, int sampleSize, int iterations)
    {
        int[] assignments = new int[count];
        int[] counts = new int[centerCount];
        short[] centers;
        if (Environment.GetEnvironmentVariable("RINHA_TRAIN_KMEANS") != "1")
        {
            Console.WriteLine($"Building bucket-projection native IVF index with {centerCount} centers");
            centers = BuildBucketAssignments(vectors, count, assignments, counts, centerCount);
        }
        else
        {
            Console.WriteLine($"Training {centerCount} IVF centers from {sampleSize:n0} samples for {iterations} iterations");
            centers = TrainCenters(vectors, count, centerCount, sampleSize, iterations);

            Console.WriteLine("Assigning references to centers");
            for (int i = 0; i < count; i++)
            {
                int center = NearestCenter(vectors.AsSpan(i * VectorSpec.PackedDimensions, VectorSpec.PackedDimensions), centers, centerCount);
                assignments[i] = center;
                counts[center]++;
            }
        }

        int[] blockCounts = new int[centerCount];
        int totalBlocks = 0;
        for (int c = 0; c < centerCount; c++)
        {
            int vectorsInCluster = counts[c];
            int blocks = (vectorsInCluster + Lanes - 1) / Lanes;
            blockCounts[c] = blocks;
            totalBlocks += blocks;
        }

        Console.WriteLine($"Total blocks: {totalBlocks}, Max cluster size: {counts.Max()}, Min cluster size: {counts.Min()}");

        int[] blockOffsets = new int[centerCount + 1];
        for (int c = 0; c < centerCount; c++)
        {
            blockOffsets[c + 1] = blockOffsets[c] + blockCounts[c];
        }

        byte[] orderedLabels = new byte[totalBlocks * Lanes];
        short[] orderedBlocks = new short[totalBlocks * VectorSpec.Dimensions * Lanes];
        int[] currentLane = new int[centerCount];

        for (int i = 0; i < count; i++)
        {
            int c = assignments[i];
            int laneIdx = currentLane[c]++;
            int blockInCluster = laneIdx / Lanes;
            int laneInBlock = laneIdx % Lanes;

            int globalBlock = blockOffsets[c] + blockInCluster;
            
            orderedLabels[globalBlock * Lanes + laneInBlock] = labels[i];

            int vectorBase = i * VectorSpec.PackedDimensions;
            int blockBase = globalBlock * VectorSpec.Dimensions * Lanes;
            for (int d = 0; d < VectorSpec.Dimensions; d++)
            {
                orderedBlocks[blockBase + d * Lanes + laneInBlock] = vectors[vectorBase + d];
            }
        }

        for (int c = 0; c < centerCount; c++)
        {
            int filledLanes = currentLane[c];
            int paddedLanes = blockCounts[c] * Lanes;
            for (int laneIdx = filledLanes; laneIdx < paddedLanes; laneIdx++)
            {
                int blockInCluster = laneIdx / Lanes;
                int laneInBlock = laneIdx % Lanes;
                int globalBlock = blockOffsets[c] + blockInCluster;

                orderedLabels[globalBlock * Lanes + laneInBlock] = 0;
            }
        }

        short[] transposedCenters = new short[centerCount * VectorSpec.Dimensions];
        for (int d = 0; d < VectorSpec.Dimensions; d++)
        {
            for (int c = 0; c < centerCount; c++)
            {
                transposedCenters[d * centerCount + c] = centers[c * VectorSpec.PackedDimensions + d];
            }
        }

        Directory.CreateDirectory(Path.GetDirectoryName(Path.GetFullPath(path))!);
        using var file = File.Create(path);

        file.Write(Magic);
        WriteInt(file, VectorSpec.FlatScale);
        WriteInt(file, VectorSpec.PackedDimensions);
        WriteInt(file, count);
        WriteInt(file, centerCount);

        file.Write(MemoryMarshal.AsBytes(transposedCenters.AsSpan()));
        
        for (int i = 0; i <= centerCount; i++)
        {
            WriteInt(file, blockOffsets[i]);
        }

        for (int i = 0; i < centerCount; i++)
        {
            WriteInt(file, counts[i]);
        }

        file.Write(orderedLabels);
        file.Write(MemoryMarshal.AsBytes(orderedBlocks.AsSpan()));
    }

    private static short[] BuildBucketAssignments(short[] vectors, int count, int[] assignments, int[] counts, int centerCount)
    {
        int[] baseCounts = new int[VectorSpec.DefaultCenters];
        for (int i = 0; i < count; i++)
        {
            int bucket = BucketFor(vectors.AsSpan(i * VectorSpec.PackedDimensions, VectorSpec.PackedDimensions));
            baseCounts[bucket]++;
        }

        int nonEmptyBuckets = baseCounts.Count(static x => x > 0);
        if (centerCount < nonEmptyBuckets)
            throw new InvalidOperationException($"IVF center count {centerCount} is smaller than non-empty bucket count {nonEmptyBuckets}.");

        int[] centersPerBucket = AllocateCenters(baseCounts, count, centerCount);
        int[] bucketStarts = new int[VectorSpec.DefaultCenters + 1];
        for (int b = 0; b < VectorSpec.DefaultCenters; b++)
            bucketStarts[b + 1] = bucketStarts[b] + centersPerBucket[b];

        var centers = new short[centerCount * VectorSpec.PackedDimensions];
        var sums = new long[centerCount * VectorSpec.PackedDimensions];

        for (int i = 0; i < count; i++)
        {
            var vector = vectors.AsSpan(i * VectorSpec.PackedDimensions, VectorSpec.PackedDimensions);
            int baseBucket = BucketFor(vector);
            int bucketCenterCount = centersPerBucket[baseBucket];
            int center = bucketStarts[baseBucket] + ProjectionFor(vector, bucketCenterCount);
            assignments[i] = center;
            counts[center]++;

            int sumBase = center * VectorSpec.PackedDimensions;
            for (int d = 0; d < VectorSpec.Dimensions; d++)
                sums[sumBase + d] += vector[d];
        }

        int firstNonEmpty = Array.FindIndex(counts, static x => x > 0);
        for (int c = 0; c < centerCount; c++)
        {
            int source = counts[c] > 0 ? c : firstNonEmpty;
            int centerBase = c * VectorSpec.PackedDimensions;
            int sumBase = source * VectorSpec.PackedDimensions;
            int divisor = Math.Max(1, counts[source]);
            for (int d = 0; d < VectorSpec.Dimensions; d++)
            {
                centers[centerBase + d] = (short)Math.Clamp(
                    sums[sumBase + d] / divisor,
                    -VectorSpec.FlatScale,
                    VectorSpec.FlatScale);
            }
        }

        return centers;
    }

    private static int[] AllocateCenters(int[] baseCounts, int count, int centerCount)
    {
        int[] centersPerBucket = new int[baseCounts.Length];
        double[] remainders = new double[baseCounts.Length];
        int allocated = 0;
        int remaining = centerCount - baseCounts.Count(static x => x > 0);

        for (int b = 0; b < baseCounts.Length; b++)
        {
            if (baseCounts[b] == 0)
                continue;

            double exactExtra = (double)baseCounts[b] * remaining / count;
            int extra = (int)Math.Floor(exactExtra);
            centersPerBucket[b] = 1 + extra;
            remainders[b] = exactExtra - extra;
            allocated += centersPerBucket[b];
        }

        while (allocated < centerCount)
        {
            int best = 0;
            for (int b = 1; b < remainders.Length; b++)
            {
                if (remainders[b] > remainders[best])
                    best = b;
            }

            centersPerBucket[best]++;
            remainders[best] = 0;
            allocated++;
        }

        while (allocated > centerCount)
        {
            int best = -1;
            for (int b = 0; b < centersPerBucket.Length; b++)
            {
                if (centersPerBucket[b] <= 1)
                    continue;
                if (best < 0 || baseCounts[b] < baseCounts[best])
                    best = b;
            }

            if (best < 0)
                break;
            centersPerBucket[best]--;
            allocated--;
        }

        return centersPerBucket;
    }

    private static short[] TrainCenters(short[] vectors, int count, int centerCount, int sampleSize, int iterations)
    {
        var centers = new short[centerCount * VectorSpec.PackedDimensions];
        var random = new Random(42);

        var samples = new int[sampleSize];
        for (int i = 0; i < sampleSize; i++)
            samples[i] = random.Next(count);

        Console.WriteLine("  Random initialization");
        for (int c = 0; c < centerCount; c++)
        {
            int firstCenter = samples[random.Next(sampleSize)];
            vectors.AsSpan(firstCenter * VectorSpec.PackedDimensions, VectorSpec.PackedDimensions)
                .CopyTo(centers.AsSpan(c * VectorSpec.PackedDimensions, VectorSpec.PackedDimensions));
        }

        var sums = new long[centerCount * VectorSpec.PackedDimensions];
        var counts = new int[centerCount];

        for (int iteration = 0; iteration < iterations; iteration++)
        {
            Array.Clear(sums);
            Array.Clear(counts);
            for (int s = 0; s < sampleSize; s++)
            {
                int item = samples[s];
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
                    centers[offset + d] = (short)Math.Clamp(
                        sums[offset + d] / counts[c], 
                        -VectorSpec.FlatScale, 
                        VectorSpec.FlatScale);
            }

            Console.WriteLine($"  iteration {iteration + 1}/{iterations}");
        }

        return centers;
    }

    private static long Distance(ReadOnlySpan<short> a, ReadOnlySpan<short> b)
    {
        long distance = 0;
        for (int d = 0; d < VectorSpec.Dimensions; d++)
        {
            long diff = a[d] - b[d];
            distance += diff * diff;
        }
        return distance;
    }

    private static int NearestCenter(ReadOnlySpan<short> vector, short[] centers, int centerCount)
    {
        int best = 0;
        long bestDistance = long.MaxValue;
        for (int c = 0; c < centerCount; c++)
        {
            long distance = 0;
            int offset = c * VectorSpec.PackedDimensions;
            for (int d = 0; d < VectorSpec.Dimensions; d++)
            {
                long diff = vector[d] - centers[offset + d];
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

    private static int BucketFor(ReadOnlySpan<short> vector)
    {
        int bucket = 0;
        if (vector[2] > VectorSpec.FlatScale / 2) bucket |= 1 << 0;
        if (vector[5] < 0) bucket |= 1 << 1;
        if (vector[7] > VectorSpec.FlatScale / 2) bucket |= 1 << 2;
        if (vector[8] > VectorSpec.FlatScale / 2) bucket |= 1 << 3;
        if (vector[9] > 0) bucket |= 1 << 4;
        if (vector[10] == 0) bucket |= 1 << 5;
        if (vector[11] > 0) bucket |= 1 << 6;
        if (vector[12] > VectorSpec.FlatScale / 2) bucket |= 1 << 7;
        if (vector[0] > VectorSpec.FlatScale / 2) bucket |= 1 << 8;
        return bucket;
    }

    private static int ProjectionFor(ReadOnlySpan<short> vector, int parts)
    {
        if (parts <= 1)
            return 0;

        const int weights = 11 + 17 + 23 + 29 + 31 + 37 + 41;
        long projection = 0;
        projection += (long)(vector[0] + VectorSpec.FlatScale) * 11;
        projection += (long)(vector[2] + VectorSpec.FlatScale) * 17;
        projection += (long)(vector[3] + VectorSpec.FlatScale) * 23;
        projection += (long)(vector[7] + VectorSpec.FlatScale) * 29;
        projection += (long)(vector[8] + VectorSpec.FlatScale) * 31;
        projection += (long)(vector[12] + VectorSpec.FlatScale) * 37;
        projection += (long)(vector[13] + VectorSpec.FlatScale) * 41;

        long maxProjection = (long)VectorSpec.FlatScale * 2 * weights;
        int split = (int)(projection * parts / (maxProjection + 1));
        return Math.Clamp(split, 0, parts - 1);
    }

    private static void WriteInt(Stream stream, int value)
    {
        Span<byte> buffer = stackalloc byte[4];
        BinaryPrimitives.WriteInt32LittleEndian(buffer, value);
        stream.Write(buffer);
    }
}
