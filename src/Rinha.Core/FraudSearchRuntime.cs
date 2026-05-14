namespace Rinha.Core;

public static class FraudSearchRuntime
{
    public static IFraudSearch CreateFromEnvironment()
    {
        string backend = Environment.GetEnvironmentVariable("RINHA_SEARCH_BACKEND") ?? "rust";
        if (string.Equals(backend, "service", StringComparison.OrdinalIgnoreCase))
        {
            string searchSocket = Environment.GetEnvironmentVariable("RINHA_SEARCH_SOCKET") ?? "/sockets/search.sock";
            return SearchServiceFraudSearch.Open(searchSocket);
        }

        string nativeIndexPath = Environment.GetEnvironmentVariable("RINHA_NATIVE_INDEX_PATH") ?? "/app/index/native.idx";
        return NativeFraudSearch.Open(nativeIndexPath);
    }
}
