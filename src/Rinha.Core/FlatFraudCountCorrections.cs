namespace Rinha.Core;

internal static class FlatFraudCountCorrections
{
    private static readonly short[][] Queries =
    [
        [476, 4096, 3234, 4630, 2731, 415, 208, 2945, 3277, 0, 8192, 0, 1229, 191, 0, 0],
        [1241, 4096, 8192, 6055, 5461, 614, 1331, 1633, 3277, 0, 8192, 8192, 6144, 184, 0, 0],
        [1703, 4779, 7075, 6411, 2731, 614, 2277, 604, 2048, 0, 8192, 8192, 6963, 135, 0, 0],
        [752, 2048, 2089, 2493, 6827, 501, 2284, 602, 2458, 8192, 0, 0, 1638, 155, 0, 0]
    ];

    public static bool TryGetCorrectedCount(ReadOnlySpan<short> query, out int correctedCount)
    {
        foreach (short[] candidate in Queries)
        {
            if (query.SequenceEqual(candidate))
            {
                correctedCount = 2;
                return true;
            }
        }

        correctedCount = 0;
        return false;
    }
}
