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

        if (backend.Equals("rust", StringComparison.OrdinalIgnoreCase))
        {
            try
            {
                return NativeFraudSearch.Open(nativeIndexPath);
            }
            catch
            {
                // Keep the .NET index as a fallback for local/debug runs where the
                // native artifact is missing, but avoid loading it on the hot path.
            }
        }

        return new DotNetFraudSearch(FraudIndex.Load(annIndexPath, probes, maxCandidates));
    }
}
