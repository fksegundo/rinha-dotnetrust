namespace Rinha.Core;

public interface IFraudSearch
{
    int PredictFraudCount(ReadOnlySpan<byte> payload);
}
