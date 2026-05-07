namespace Rinha.Core;

public static class VectorSpec
{
    public const int Dimensions = 14;
    public const int PackedDimensions = 16;
    public const int Scale = 100;
    public const int FlatScale = 8192;
    public const int DefaultCenters = 512;
    public const int DefaultSampleSize = 30_000;
    public const int DefaultIterations = 5;
    public const int DefaultProbes = 2;
    public const int DefaultMaxCandidatesPerCenter = 768;

    public static sbyte Quantize(double value)
    {
        if (value <= -1.0)
            return -Scale;
        if (value <= 0)
            return 0;
        if (value >= 1)
            return Scale;
        return (sbyte)Math.Round(value * Scale);
    }

    public static sbyte QuantizeClamped(double value)
    {
        if (value <= 0)
            return 0;
        if (value >= 1)
            return Scale;
        return (sbyte)Math.Round(value * Scale);
    }

    public static short QuantizeFlat(double value)
    {
        if (value <= -1.0)
            return -FlatScale;
        if (value <= 0)
            return 0;
        if (value >= 1)
            return FlatScale;
        return (short)Math.Round(value * FlatScale);
    }

    public static short ToFlat(sbyte value)
    {
        int scaled = value * FlatScale;
        if (scaled >= 0)
            scaled += Scale / 2;
        else
            scaled -= Scale / 2;
        return (short)(scaled / Scale);
    }
}
