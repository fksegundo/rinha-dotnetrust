using System.Buffers.Binary;
using Rinha.Core;

internal static class NativeIndexWriter
{
    private static readonly byte[] Magic = "RNATIDX2"u8.ToArray();

    public static void Write(string path, short[] vectors, byte[] labels, int count, int leafSize)
    {
        leafSize = Math.Clamp(leafSize, 32, 2048);

        var partitions = new List<int>[256];
        for (int i = 0; i < partitions.Length; i++)
            partitions[i] = [];

        for (int i = 0; i < count; i++)
        {
            int key = PartitionKey(vectors.AsSpan(i * VectorSpec.PackedDimensions, VectorSpec.PackedDimensions));
            partitions[key].Add(i);
        }

        var orderedBlocks = new List<short>(capacity: count * VectorSpec.Dimensions * 2);
        var orderedLabels = new List<byte>(capacity: count * 2);
        var nodes = new List<NodeInfo>(capacity: Math.Max(1024, count / leafSize * 2));
        var partitionHeaders = new List<PartitionInfo>(capacity: partitions.Length);

        for (int key = 0; key < partitions.Length; key++)
        {
            if (partitions[key].Count == 0)
                continue;

            int[] indices = [.. partitions[key]];
            int rootNode = BuildNode(vectors, labels, indices, 0, indices.Length, leafSize, orderedBlocks, orderedLabels, nodes);
            NodeInfo root = nodes[rootNode];
            partitionHeaders.Add(new PartitionInfo(key, rootNode, root.Start, root.Length, root.Min, root.Max));
        }

        Directory.CreateDirectory(Path.GetDirectoryName(Path.GetFullPath(path))!);
        using var file = File.Create(path);

        file.Write(Magic);
        WriteInt(file, VectorSpec.FlatScale);
        WriteInt(file, VectorSpec.PackedDimensions);
        WriteInt(file, count);
        WriteInt(file, leafSize);
        WriteInt(file, partitionHeaders.Count);
        WriteInt(file, nodes.Count);
        WriteInt(file, orderedLabels.Count / 8); // totalBlocks

        foreach (PartitionInfo partition in partitionHeaders)
        {
            WriteInt(file, partition.Key);
            WriteInt(file, partition.RootNode);
            WriteInt(file, partition.Start);
            WriteInt(file, partition.Length);
            WriteShorts(file, partition.Min);
            WriteShorts(file, partition.Max);
        }

        foreach (NodeInfo node in nodes)
        {
            WriteInt(file, node.Left);
            WriteInt(file, node.Right);
            WriteInt(file, node.Start);
            WriteInt(file, node.Length);
            WriteShorts(file, node.Min);
            WriteShorts(file, node.Max);
        }

        file.Write(System.Runtime.InteropServices.MemoryMarshal.AsBytes(System.Runtime.InteropServices.CollectionsMarshal.AsSpan(orderedBlocks)));
        file.Write(System.Runtime.InteropServices.CollectionsMarshal.AsSpan(orderedLabels));
    }

    private static int BuildNode(
        short[] vectors,
        byte[] labels,
        int[] indices,
        int start,
        int length,
        int leafSize,
        List<short> orderedBlocks,
        List<byte> orderedLabels,
        List<NodeInfo> nodes)
    {
        short[] min = CreateBounds(initialize: short.MaxValue);
        short[] max = CreateBounds(initialize: short.MinValue);
        UpdateBounds(vectors, indices, start, length, min, max);

        int nodeIndex = nodes.Count;
        nodes.Add(default!);

        if (length <= leafSize)
        {
            int leafStart = orderedLabels.Count / 8;
            int blocks = (length + 7) / 8;

            for (int b = 0; b < blocks; b++)
            {
                for (int l = 0; l < 8; l++)
                {
                    int i = b * 8 + l;
                    if (i < length)
                        orderedLabels.Add(labels[indices[start + i]]);
                    else
                        orderedLabels.Add(0);
                }

                for (int d = 0; d < VectorSpec.Dimensions; d++)
                {
                    for (int l = 0; l < 8; l++)
                    {
                        int i = b * 8 + l;
                        if (i < length)
                        {
                            int sourceIndex = indices[start + i];
                            orderedBlocks.Add(vectors[sourceIndex * VectorSpec.PackedDimensions + d]);
                        }
                        else
                        {
                            orderedBlocks.Add(0);
                        }
                    }
                }
            }

            nodes[nodeIndex] = new NodeInfo(-1, -1, leafStart, length, min, max);
            return nodeIndex;
        }

        int splitDimension = WidestDimension(min, max);
        Array.Sort(indices, start, length, Comparer<int>.Create((left, right) =>
            vectors[left * VectorSpec.PackedDimensions + splitDimension].CompareTo(
                vectors[right * VectorSpec.PackedDimensions + splitDimension])));

        int leftLength = length / 2;
        int rightLength = length - leftLength;
        int leftNode = BuildNode(vectors, labels, indices, start, leftLength, leafSize, orderedBlocks, orderedLabels, nodes);
        int rightNode = BuildNode(vectors, labels, indices, start + leftLength, rightLength, leafSize, orderedBlocks, orderedLabels, nodes);

        NodeInfo leftInfo = nodes[leftNode];
        NodeInfo rightInfo = nodes[rightNode];
        nodes[nodeIndex] = new NodeInfo(leftNode, rightNode, leftInfo.Start, leftInfo.Length + rightInfo.Length, min, max);
        return nodeIndex;
    }

    private static int PartitionKey(ReadOnlySpan<short> vector)
    {
        int key = 0;
        if (vector[5] >= 0) key |= 1 << 0;  // has_last_tx
        if (vector[9] > 0)  key |= 1 << 1;  // is_online
        if (vector[10] > 0) key |= 1 << 2;  // card_present
        if (vector[11] > 0) key |= 1 << 3;  // unknown_merchant
        int mccBucket = vector[12] switch
        {
            < 2048 => 0,
            < 4096 => 1,
            < 6144 => 2,
            _ => 3
        };
        key |= mccBucket << 4;
        if (vector[2] > 4096) key |= 1 << 6; // amount > 5x customer avg (suspicious ratio)
        if (vector[8] > 2048) key |= 1 << 7; // tx_count_24h > 5 (high frequency)
        return key;
    }

    private static void UpdateBounds(short[] vectors, int[] indices, int start, int length, short[] min, short[] max)
    {
        for (int i = 0; i < length; i++)
        {
            int vectorBase = indices[start + i] * VectorSpec.PackedDimensions;
            for (int d = 0; d < VectorSpec.PackedDimensions; d++)
            {
                short value = vectors[vectorBase + d];
                if (value < min[d])
                    min[d] = value;
                if (value > max[d])
                    max[d] = value;
            }
        }
    }

    private static int WidestDimension(short[] min, short[] max)
    {
        int bestDimension = 0;
        int bestWidth = int.MinValue;
        for (int d = 0; d < VectorSpec.Dimensions; d++)
        {
            int width = max[d] - min[d];
            if (width > bestWidth)
            {
                bestWidth = width;
                bestDimension = d;
            }
        }

        return bestDimension;
    }

    private static short[] CreateBounds(short initialize)
    {
        var values = new short[VectorSpec.PackedDimensions];
        Array.Fill(values, initialize);
        return values;
    }

    private static void WriteInt(Stream stream, int value)
    {
        Span<byte> buffer = stackalloc byte[4];
        BinaryPrimitives.WriteInt32LittleEndian(buffer, value);
        stream.Write(buffer);
    }

    private static void WriteShorts(Stream stream, short[] values)
    {
        stream.Write(System.Runtime.InteropServices.MemoryMarshal.AsBytes(values.AsSpan()));
    }

    private sealed record NodeInfo(int Left, int Right, int Start, int Length, short[] Min, short[] Max);

    private sealed record PartitionInfo(int Key, int RootNode, int Start, int Length, short[] Min, short[] Max);
}
