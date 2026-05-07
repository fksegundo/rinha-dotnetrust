using System.Buffers.Binary;
using System.Runtime.InteropServices;
using System.Runtime.Intrinsics;

namespace Rinha.Core;

public sealed class FraudIndex
{
    private static readonly byte[] AnnMagic = "RIDX10A1"u8.ToArray();
    private static readonly byte[] FlatMagic = "RIDX10F1"u8.ToArray();

    private readonly sbyte[] _centers;
    private readonly int[] _offsets;
    private readonly sbyte[] _vectors;
    private readonly byte[] _labels;
    private readonly short[] _flatVectors;
    private readonly byte[] _flatLabels;
    private readonly int _centerCount;
    private readonly int _flatCount;
    private readonly int _probes;
    private readonly int _maxCandidatesPerCenter;

    private FraudIndex(
        sbyte[] centers,
        int[] offsets,
        sbyte[] vectors,
        byte[] labels,
        short[] flatVectors,
        byte[] flatLabels,
        int centerCount,
        int flatCount,
        int probes,
        int maxCandidatesPerCenter)
    {
        _centers = centers;
        _offsets = offsets;
        _vectors = vectors;
        _labels = labels;
        _flatVectors = flatVectors;
        _flatLabels = flatLabels;
        _centerCount = centerCount;
        _flatCount = flatCount;
        _probes = centerCount > 0 ? Math.Clamp(probes, 1, centerCount) : 1;
        _maxCandidatesPerCenter = Math.Max(5, maxCandidatesPerCenter);
    }

    public static FraudIndex Load(string path, int probes, int maxCandidatesPerCenter)
    {
        byte[] bytes = File.ReadAllBytes(path);
        var span = bytes.AsSpan();
        if (span[..FlatMagic.Length].SequenceEqual(FlatMagic))
            return LoadFlat(bytes, probes, maxCandidatesPerCenter);

        if (!span[..AnnMagic.Length].SequenceEqual(AnnMagic))
            throw new InvalidDataException("Invalid index magic.");

        int cursor = AnnMagic.Length;
        int scale = ReadInt(span, ref cursor);
        int packedDimensions = ReadInt(span, ref cursor);
        int centerCount = ReadInt(span, ref cursor);
        int count = ReadInt(span, ref cursor);

        if (scale != VectorSpec.Scale || packedDimensions != VectorSpec.PackedDimensions)
            throw new InvalidDataException("Incompatible index format.");

        int[] offsets = new int[centerCount + 1];
        for (int i = 0; i < offsets.Length; i++)
            offsets[i] = ReadInt(span, ref cursor);

        sbyte[] centers = new sbyte[centerCount * VectorSpec.PackedDimensions];
        Buffer.BlockCopy(bytes, cursor, centers, 0, centers.Length);
        cursor += centers.Length;

        sbyte[] vectors = new sbyte[count * VectorSpec.PackedDimensions];
        Buffer.BlockCopy(bytes, cursor, vectors, 0, vectors.Length);
        cursor += vectors.Length;

        byte[] labels = new byte[count];
        Buffer.BlockCopy(bytes, cursor, labels, 0, labels.Length);

        return new FraudIndex(centers, offsets, vectors, labels, [], [], centerCount, 0, probes, maxCandidatesPerCenter);
    }

    private static FraudIndex LoadFlat(byte[] bytes, int probes, int maxCandidatesPerCenter)
    {
        var span = bytes.AsSpan();
        int cursor = FlatMagic.Length;
        int scale = ReadInt(span, ref cursor);
        int packedDimensions = ReadInt(span, ref cursor);
        int count = ReadInt(span, ref cursor);

        if (scale != VectorSpec.FlatScale || packedDimensions != VectorSpec.PackedDimensions)
            throw new InvalidDataException("Incompatible flat index format.");

        short[] vectors = new short[count * VectorSpec.PackedDimensions];
        Buffer.BlockCopy(bytes, cursor, vectors, 0, vectors.Length * sizeof(short));
        cursor += vectors.Length * sizeof(short);

        byte[] labels = new byte[count];
        Buffer.BlockCopy(bytes, cursor, labels, 0, labels.Length);

        return new FraudIndex([], [], [], [], vectors, labels, 0, count, probes, maxCandidatesPerCenter);
    }

    public static void Write(string path, int centerCount, int[] offsets, sbyte[] centers, sbyte[] vectors, byte[] labels)
    {
        Directory.CreateDirectory(Path.GetDirectoryName(Path.GetFullPath(path))!);
        using var file = File.Create(path);
        file.Write(AnnMagic);
        WriteInt(file, VectorSpec.Scale);
        WriteInt(file, VectorSpec.PackedDimensions);
        WriteInt(file, centerCount);
        WriteInt(file, labels.Length);
        foreach (int offset in offsets)
            WriteInt(file, offset);
        file.Write(MemoryMarshalBytes(centers));
        file.Write(MemoryMarshalBytes(vectors));
        file.Write(labels);
    }

    public static void WriteFlat(string path, short[] vectors, byte[] labels)
    {
        Directory.CreateDirectory(Path.GetDirectoryName(Path.GetFullPath(path))!);
        using var file = File.Create(path);
        file.Write(FlatMagic);
        WriteInt(file, VectorSpec.FlatScale);
        WriteInt(file, VectorSpec.PackedDimensions);
        WriteInt(file, labels.Length);
        file.Write(MemoryMarshal.AsBytes(vectors.AsSpan()));
        file.Write(labels);
    }

    public int PredictFraudCount(ReadOnlySpan<sbyte> query)
    {
        if (_flatCount > 0)
            return PredictFlatFraudCount(query);

        Span<int> bestCenterDistance = stackalloc int[16];
        Span<int> bestCenter = stackalloc int[16];
        int probeCount = Math.Min(_probes, bestCenter.Length);
        bestCenterDistance[..probeCount].Fill(int.MaxValue);
        bestCenter[..probeCount].Fill(-1);

        for (int c = 0; c < _centerCount; c++)
        {
            if (_offsets[c] == _offsets[c + 1])
                continue;

            int distance = Distance(query, _centers.AsSpan(c * VectorSpec.PackedDimensions, VectorSpec.PackedDimensions), int.MaxValue);
            InsertBest(distance, c, bestCenterDistance[..probeCount], bestCenter[..probeCount]);
        }

        Span<int> bestDistance = stackalloc int[5];
        Span<byte> bestLabel = stackalloc byte[5];
        bestDistance.Fill(int.MaxValue);
        int queryHash = QueryHash(query);

        for (int probe = 0; probe < probeCount; probe++)
        {
            int center = bestCenter[probe];
            if (center < 0)
                continue;

            int start = _offsets[center];
            int end = _offsets[center + 1];
            int length = end - start;
            int scanCount = Math.Min(length, _maxCandidatesPerCenter);
            int step = Math.Max(1, length / scanCount);
            int offset = step == 1 ? 0 : Math.Abs(queryHash + center * 31) % step;

            for (int seen = 0, i = start + offset; seen < scanCount && i < end; seen++, i += step)
            {
                int distance = Distance(query, _vectors.AsSpan(i * VectorSpec.PackedDimensions, VectorSpec.PackedDimensions), bestDistance[4]);
                if (distance < bestDistance[4])
                    InsertBest(distance, _labels[i], bestDistance, bestLabel);
            }
        }

        int frauds = 0;
        for (int i = 0; i < 5; i++)
            frauds += bestLabel[i];
        return frauds;
    }

    private int PredictFlatFraudCount(ReadOnlySpan<sbyte> query)
    {
        Span<short> flatQuery = stackalloc short[VectorSpec.PackedDimensions];
        for (int i = 0; i < VectorSpec.Dimensions; i++)
            flatQuery[i] = ToFlat(query[i]);

        Span<int> bestDistance = stackalloc int[5];
        Span<byte> bestLabel = stackalloc byte[5];
        bestDistance.Fill(int.MaxValue);

        var qVec = Vector256.LoadUnsafe(ref MemoryMarshal.GetReference(flatQuery));
        ref short vectorBase = ref MemoryMarshal.GetArrayDataReference(_flatVectors);
        int bound = int.MaxValue;

        for (int i = 0; i < _flatCount; i++)
        {
            var item = Vector256.LoadUnsafe(ref vectorBase, (nuint)(i * VectorSpec.PackedDimensions));
            var diff = qVec - item;
            var (lo, hi) = Vector256.Widen(diff);
            var squared = (lo * lo) + (hi * hi);
            int distance = Vector256.Sum(squared);

            if (distance >= bound)
                continue;

            InsertBest(distance, _flatLabels[i], bestDistance, bestLabel);
            bound = bestDistance[4];
        }

        int frauds = 0;
        for (int i = 0; i < 5; i++)
            frauds += bestLabel[i];
        return frauds;
    }

    private static short ToFlat(sbyte value)
    {
        return VectorSpec.ToFlat(value);
    }

    private static int QueryHash(ReadOnlySpan<sbyte> query)
    {
        unchecked
        {
            int hash = 17;
            for (int i = 0; i < VectorSpec.Dimensions; i++)
                hash = hash * 31 + query[i];
            return hash & 0x7fffffff;
        }
    }

    private static int Distance(ReadOnlySpan<sbyte> a, ReadOnlySpan<sbyte> b, int stopAt)
    {
        if (a.Length >= VectorSpec.PackedDimensions && b.Length >= VectorSpec.PackedDimensions)
        {
            var aVec = Vector128.LoadUnsafe(ref MemoryMarshal.GetReference(a));
            var bVec = Vector128.LoadUnsafe(ref MemoryMarshal.GetReference(b));
            var (aLo, aHi) = Vector128.Widen(aVec);
            var (bLo, bHi) = Vector128.Widen(bVec);

            var diffLo = aLo - bLo;
            var diffHi = aHi - bHi;
            var (lo0, lo1) = Vector128.Widen(diffLo);
            var (hi0, hi1) = Vector128.Widen(diffHi);

            var sq0 = lo0 * lo0;
            var sq1 = lo1 * lo1;
            var sq2 = hi0 * hi0;
            var sq3 = hi1 * hi1;
            return Vector128.Sum(sq0) + Vector128.Sum(sq1) + Vector128.Sum(sq2) + Vector128.Sum(sq3);
        }

        int sum = 0;
        for (int i = 0; i < VectorSpec.Dimensions; i++)
        {
            int diff = a[i] - b[i];
            sum += diff * diff;
            if (sum >= stopAt)
                return sum;
        }
        return sum;
    }

    private static void InsertBest(int distance, int value, Span<int> distances, Span<int> values)
    {
        int pos = distances.Length - 1;
        if (distance >= distances[pos])
            return;

        while (pos > 0 && distance < distances[pos - 1])
        {
            distances[pos] = distances[pos - 1];
            values[pos] = values[pos - 1];
            pos--;
        }

        distances[pos] = distance;
        values[pos] = value;
    }

    private static void InsertBest(int distance, byte label, Span<int> distances, Span<byte> labels)
    {
        int pos = distances.Length - 1;
        while (pos > 0 && distance < distances[pos - 1])
        {
            distances[pos] = distances[pos - 1];
            labels[pos] = labels[pos - 1];
            pos--;
        }

        distances[pos] = distance;
        labels[pos] = label;
    }

    private static int ReadInt(ReadOnlySpan<byte> span, ref int cursor)
    {
        int value = BinaryPrimitives.ReadInt32LittleEndian(span.Slice(cursor, 4));
        cursor += 4;
        return value;
    }

    private static void WriteInt(Stream stream, int value)
    {
        Span<byte> buffer = stackalloc byte[4];
        BinaryPrimitives.WriteInt32LittleEndian(buffer, value);
        stream.Write(buffer);
    }

    private static ReadOnlySpan<byte> MemoryMarshalBytes(sbyte[] values)
    {
        return System.Runtime.InteropServices.MemoryMarshal.AsBytes(values.AsSpan());
    }
}
