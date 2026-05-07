namespace Rinha.Core;

public static class FraudSearchRuntime
{
    public static IFraudSearch CreateFromEnvironment()
    {
        string backend = Environment.GetEnvironmentVariable("RINHA_SEARCH_BACKEND") ?? "dotnet";
        string annIndexPath = Environment.GetEnvironmentVariable("RINHA_INDEX_PATH") ?? "/app/index/rinha.idx";
        string nativeIndexPath = Environment.GetEnvironmentVariable("RINHA_NATIVE_INDEX_PATH") ?? "/app/index/native.idx";
        int probes = int.TryParse(Environment.GetEnvironmentVariable("RINHA_PROBES"), out int configuredProbes)
            ? configuredProbes
            : VectorSpec.DefaultProbes;
        int maxCandidates = int.TryParse(Environment.GetEnvironmentVariable("RINHA_MAX_CANDIDATES_PER_CENTER"), out int configuredMaxCandidates)
            ? configuredMaxCandidates
            : VectorSpec.DefaultMaxCandidatesPerCenter;

        var fallback = new DotNetFraudSearch(FraudIndex.Load(annIndexPath, probes, maxCandidates));

        if (!backend.Equals("rust", StringComparison.OrdinalIgnoreCase))
            return fallback;

        try
        {
            return NativeFraudSearch.Open(nativeIndexPath);
        }
        catch
        {
            return fallback;
        }
    }
}
