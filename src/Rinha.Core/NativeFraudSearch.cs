using System.Runtime.InteropServices;

namespace Rinha.Core;

public sealed partial class NativeFraudSearch : IFraudSearch, IDisposable
{
    private IntPtr _handle;

    private NativeFraudSearch(IntPtr handle)
    {
        _handle = handle;
    }

    public static NativeFraudSearch Open(string path)
    {
        IntPtr handle = NativeMethods.rinha_index_open(path);
        if (handle == IntPtr.Zero)
            throw new InvalidOperationException($"Unable to open native index at '{path}'.");
        return new NativeFraudSearch(handle);
    }

    public int PredictFraudCount(ReadOnlySpan<byte> payload)
    {
        Span<short> flatQuery = stackalloc short[VectorSpec.PackedDimensions];
        if (!FraudVectorizer.TryVectorizeFlat(payload, flatQuery))
            return 0;

        ref short queryRef = ref MemoryMarshal.GetReference(flatQuery);
        int result = NativeMethods.rinha_predict(_handle, ref queryRef, VectorSpec.PackedDimensions);
        if (result < 0 || result > 5)
            throw new InvalidOperationException($"Native search returned invalid fraud count '{result}'.");
        return result;
    }

    public void Dispose()
    {
        if (_handle == IntPtr.Zero)
            return;

        NativeMethods.rinha_index_close(_handle);
        _handle = IntPtr.Zero;
        GC.SuppressFinalize(this);
    }

    ~NativeFraudSearch()
    {
        Dispose();
    }

    private static partial class NativeMethods
    {
        [LibraryImport("rinha_native", StringMarshalling = StringMarshalling.Utf8)]
        internal static partial IntPtr rinha_index_open(string path);

        [LibraryImport("rinha_native")]
        internal static partial int rinha_predict(IntPtr handle, ref short query, int len);

        [LibraryImport("rinha_native")]
        internal static partial void rinha_index_close(IntPtr handle);
    }
}
