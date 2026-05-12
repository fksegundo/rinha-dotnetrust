namespace Rinha.Core;

public static class VectorSpec
{
    public const int Dimensions = 14;
    public const int PackedDimensions = 16;
    public const int FlatScale = 8192;

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
}
