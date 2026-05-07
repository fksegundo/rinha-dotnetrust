namespace Rinha.Core;

public sealed class DotNetFraudSearch(FraudIndex index) : IFraudSearch
{
    public int PredictFraudCount(ReadOnlySpan<byte> payload)
    {
        Span<sbyte> query = stackalloc sbyte[VectorSpec.PackedDimensions];
        return FraudVectorizer.TryVectorize(payload, query)
            ? index.PredictFraudCount(query)
            : 0;
    }
}
