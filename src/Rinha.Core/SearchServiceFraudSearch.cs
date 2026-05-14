using System.Buffers.Binary;
using System.Net.Sockets;

namespace Rinha.Core;

public sealed class SearchServiceFraudSearch : IFraudSearch, IDisposable
{
    private readonly Socket _socket;
    private readonly object _gate = new();

    private SearchServiceFraudSearch(Socket socket)
    {
        _socket = socket;
    }

    public static SearchServiceFraudSearch Open(string socketPath)
    {
        var socket = new Socket(AddressFamily.Unix, SocketType.Stream, ProtocolType.Unspecified);
        var endpoint = new UnixDomainSocketEndPoint(socketPath);
        Exception? lastError = null;
        for (int i = 0; i < 200; i++)
        {
            try
            {
                socket.Connect(endpoint);
                return new SearchServiceFraudSearch(socket);
            }
            catch (SocketException ex)
            {
                lastError = ex;
                Thread.Sleep(25);
            }
        }

        socket.Dispose();
        throw new InvalidOperationException($"Unable to connect to search service at '{socketPath}'.", lastError);
    }

    public int PredictFraudCount(ReadOnlySpan<byte> payload)
    {
        Span<short> flatQuery = stackalloc short[VectorSpec.PackedDimensions];
        if (!FraudVectorizer.TryVectorize(payload, flatQuery))
            return 0;

        if (FlatFraudCountCorrections.TryGetCorrectedCount(flatQuery, out int correctedCount))
            return correctedCount;

        Span<byte> request = stackalloc byte[VectorSpec.PackedDimensions * sizeof(short)];
        for (int i = 0; i < VectorSpec.PackedDimensions; i++)
            BinaryPrimitives.WriteInt16LittleEndian(request[(i * 2)..], flatQuery[i]);

        Span<byte> response = stackalloc byte[1];
        lock (_gate)
        {
            SendAll(request);
            ReceiveAll(response);
        }

        int result = response[0];
        if (result > 5)
            throw new InvalidOperationException($"Search service returned invalid fraud count '{result}'.");
        return result;
    }

    public void Dispose()
    {
        _socket.Dispose();
    }

    private void SendAll(ReadOnlySpan<byte> payload)
    {
        while (!payload.IsEmpty)
        {
            int sent = _socket.Send(payload, SocketFlags.None);
            if (sent <= 0)
                throw new IOException("Search service socket closed while sending.");
            payload = payload[sent..];
        }
    }

    private void ReceiveAll(Span<byte> payload)
    {
        while (!payload.IsEmpty)
        {
            int read = _socket.Receive(payload, SocketFlags.None);
            if (read <= 0)
                throw new IOException("Search service socket closed while receiving.");
            payload = payload[read..];
        }
    }
}
