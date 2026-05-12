namespace Rinha.Core;

public static class FraudSearchRuntime
{
    public static IFraudSearch CreateFromEnvironment()
    {
        string nativeIndexPath = Environment.GetEnvironmentVariable("RINHA_NATIVE_INDEX_PATH") ?? "/app/index/native.idx";
        return NativeFraudSearch.Open(nativeIndexPath);
    }
}
