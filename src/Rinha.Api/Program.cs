using System.Buffers;
using System.Net;
using System.Net.Sockets;
using System.Text;
using Rinha.Core;

ApplyRuntimeTuning();

string? socketPath = Environment.GetEnvironmentVariable("RINHA_SOCKET");
var fraudSearch = FraudSearchRuntime.CreateFromEnvironment();
Warmup(fraudSearch);

Socket listenSocket;
EndPoint localEndPoint;

if (!string.IsNullOrWhiteSpace(socketPath))
{
    Directory.CreateDirectory(Path.GetDirectoryName(socketPath)!);
    if (File.Exists(socketPath))
        File.Delete(socketPath);
    listenSocket = new Socket(AddressFamily.Unix, SocketType.Stream, ProtocolType.Unspecified);
    localEndPoint = new UnixDomainSocketEndPoint(socketPath);
}
else
{
    listenSocket = new Socket(AddressFamily.InterNetwork, SocketType.Stream, ProtocolType.Tcp);
    listenSocket.SetSocketOption(SocketOptionLevel.Socket, SocketOptionName.ReuseAddress, true);
    localEndPoint = new IPEndPoint(IPAddress.Any, 9999);
}

listenSocket.Bind(localEndPoint);
listenSocket.Listen(4096);

bool isTcp = listenSocket.AddressFamily == AddressFamily.InterNetwork || listenSocket.AddressFamily == AddressFamily.InterNetworkV6;
while (true)
{
    Socket client = listenSocket.Accept();
    if (isTcp)
        client.NoDelay = true;
    _ = Task.Run(() => HandleClient(client, fraudSearch));
}

static void HandleClient(Socket socket, IFraudSearch fraudSearch)
{
    byte[] buffer = ArrayPool<byte>.Shared.Rent(8192);
    Span<byte> writeBuf = stackalloc byte[256];
    try
    {
        int offset = 0;
        bool keepAlive = true;

        while (keepAlive && socket.Connected)
        {
            // ---- read headers until \r\n\r\n ---------------------------------
            int headerEnd = -1;
            while (headerEnd == -1)
            {
                int read = socket.Receive(buffer, offset, buffer.Length - offset, SocketFlags.None);
                if (read == 0) return;
                offset += read;

                ReadOnlySpan<byte> search = buffer.AsSpan(0, offset);
                for (int i = 3; i < search.Length; i++)
                {
                    if (search[i] == '\n' && search[i - 1] == '\r' && search[i - 2] == '\n' && search[i - 3] == '\r')
                    {
                        headerEnd = i + 1;
                        break;
                    }
                }
                if (offset >= buffer.Length) return; // too large
            }

            // ---- parse first line (METHOD PATH HTTP/1.x) --------------------
            ReadOnlySpan<byte> headerSpan = buffer.AsSpan(0, headerEnd);
            int firstLineEnd = headerSpan.IndexOf("\r\n"u8);
            if (firstLineEnd < 0) return;
            ReadOnlySpan<byte> firstLine = headerSpan.Slice(0, firstLineEnd);

            bool isPost = firstLine.Length > 4 && firstLine[0] == (byte)'P' && firstLine[1] == (byte)'O';
            bool isGet = !isPost && firstLine.Length > 3 && firstLine[0] == (byte)'G' && firstLine[1] == (byte)'E';

            int pathStart = firstLine.IndexOf((byte)' ') + 1;
            int pathEnd = firstLine.Slice(pathStart).IndexOf((byte)' ');
            ReadOnlySpan<byte> path = firstLine.Slice(pathStart, pathEnd);
            bool isReady = path.Length == 6 && path[1] == (byte)'r'; // /ready
            bool isFraud = path.Length == 12 && path[1] == (byte)'f' && path[6] == (byte)'-' && path[7] == (byte)'s'; // /fraud-score

            // ---- extract Content-Length -----------------------------------
            int contentLength = 0;
            if (isPost)
            {
                ReadOnlySpan<byte> clHeader = "content-length: "u8;
                int clPos = headerSpan.IndexOf(clHeader);
                if (clPos >= 0)
                {
                    int numStart = clPos + clHeader.Length;
                    int numEnd = headerSpan.Slice(numStart).IndexOf("\r\n"u8);
                    if (numEnd > 0)
                    {
                        ReadOnlySpan<byte> numSpan = headerSpan.Slice(numStart, numEnd);
                        contentLength = ParseInt(numSpan);
                    }
                }
            }

            // ---- extract Connection ---------------------------------------
            ReadOnlySpan<byte> connHeader = "connection: close"u8;
            bool connectionClose = headerSpan.IndexOf(connHeader) >= 0;

            // ---- read body ------------------------------------------------
            int bodyStart = headerEnd;
            int totalNeeded = bodyStart + contentLength;
            while (offset < totalNeeded)
            {
                int read = socket.Receive(buffer, offset, buffer.Length - offset, SocketFlags.None);
                if (read == 0) return;
                offset += read;
                if (offset >= buffer.Length) return;
            }

            // ---- process request ------------------------------------------
            ReadOnlyMemory<byte> responseBody;
            string contentType = "application/json";

            if (isGet && isReady)
            {
                responseBody = Responses.Ready;
                contentType = "text/plain";
            }
            else if (isPost && isFraud)
            {
                ReadOnlySpan<byte> bodySpan = buffer.AsSpan(bodyStart, contentLength);
                int frauds = fraudSearch.PredictFraudCount(bodySpan);
                responseBody = Responses.ByFraudCount[frauds];
            }
            else
            {
                responseBody = Responses.ByFraudCount[0];
            }

            // ---- write response -------------------------------------------
            ReadOnlySpan<byte> statusLine = connectionClose
                ? "HTTP/1.1 200 OK\r\nConnection: close\r\nContent-Type: "u8
                : "HTTP/1.1 200 OK\r\nConnection: keep-alive\r\nContent-Type: "u8;

            int pos = 0;
            statusLine.CopyTo(writeBuf.Slice(pos));
            pos += statusLine.Length;
            ReadOnlySpan<byte> ctBytes = Encoding.UTF8.GetBytes(contentType);
            ctBytes.CopyTo(writeBuf.Slice(pos));
            pos += ctBytes.Length;
            ReadOnlySpan<byte> lenPrefix = "\r\nContent-Length: "u8;
            lenPrefix.CopyTo(writeBuf.Slice(pos));
            pos += lenPrefix.Length;
            pos += WriteInt(writeBuf.Slice(pos), responseBody.Length);
            ReadOnlySpan<byte> sep = "\r\n\r\n"u8;
            sep.CopyTo(writeBuf.Slice(pos));
            pos += sep.Length;

            socket.Send(writeBuf.Slice(0, pos));
            socket.Send(responseBody.Span);

            // ---- shift remaining bytes for next request --------------------
            int consumed = totalNeeded;
            if (offset > consumed)
            {
                Buffer.BlockCopy(buffer, consumed, buffer, 0, offset - consumed);
                offset -= consumed;
            }
            else
            {
                offset = 0;
            }

            keepAlive = !connectionClose;
        }
    }
    finally
    {
        ArrayPool<byte>.Shared.Return(buffer);
        socket.Close();
    }
}

static int ParseInt(ReadOnlySpan<byte> span)
{
    int value = 0;
    foreach (byte b in span)
    {
        if (b >= (byte)'0' && b <= (byte)'9')
            value = value * 10 + (b - (byte)'0');
    }
    return value;
}

static int WriteInt(Span<byte> dest, int value)
{
    if (value == 0)
    {
        dest[0] = (byte)'0';
        return 1;
    }
    int pos = 0;
    int temp = value;
    while (temp > 0)
    {
        pos++;
        temp /= 10;
    }
    int end = pos;
    temp = value;
    while (temp > 0)
    {
        dest[--pos] = (byte)('0' + (temp % 10));
        temp /= 10;
    }
    return end;
}

static void ApplyRuntimeTuning()
{
    if (!int.TryParse(Environment.GetEnvironmentVariable("RINHA_MIN_THREADS"), out int configured))
        return;

    int minThreads = Math.Clamp(configured, 1, 64);
    ThreadPool.GetMinThreads(out _, out int minIoThreads);
    ThreadPool.SetMinThreads(minThreads, minIoThreads);
}

static void Warmup(IFraudSearch fraudSearch)
{
    foreach (ReadOnlyMemory<byte> payload in WarmupPayloads.All)
    {
        for (int i = 0; i < 500; i++)
            _ = fraudSearch.PredictFraudCount(payload.Span);
    }
}

internal static class Responses
{
    public static readonly ReadOnlyMemory<byte> Ready = "ok"u8.ToArray();

    public static readonly ReadOnlyMemory<byte>[] ByFraudCount =
    [
        """{"approved":true,"fraud_score":0.0}"""u8.ToArray(),
        """{"approved":true,"fraud_score":0.2}"""u8.ToArray(),
        """{"approved":true,"fraud_score":0.4}"""u8.ToArray(),
        """{"approved":false,"fraud_score":0.6}"""u8.ToArray(),
        """{"approved":false,"fraud_score":0.8}"""u8.ToArray(),
        """{"approved":false,"fraud_score":1.0}"""u8.ToArray()
    ];
}

internal static class WarmupPayloads
{
    public static readonly ReadOnlyMemory<byte>[] All =
    [
        """
        {"transaction":{"amount":120.5,"installments":1,"requested_at":"2026-01-05T10:15:00Z"},"customer":{"avg_amount":95.0,"tx_count_24h":3,"known_merchants":["m1","m2"]},"merchant":{"id":"m1","mcc":"5411","avg_amount":110.0},"terminal":{"is_online":true,"card_present":false,"km_from_home":12.0},"last_transaction":{"timestamp":"2026-01-05T09:40:00Z","km_from_current":4.0}}
        """u8.ToArray(),
        """
        {"transaction":{"amount":890.0,"installments":6,"requested_at":"2026-01-06T23:50:00Z"},"customer":{"avg_amount":120.0,"tx_count_24h":9,"known_merchants":["m7"]},"merchant":{"id":"m9","mcc":"7995","avg_amount":500.0},"terminal":{"is_online":true,"card_present":true,"km_from_home":220.0},"last_transaction":null}
        """u8.ToArray(),
        """
        {"transaction":{"amount":45.0,"installments":1,"requested_at":"2026-01-07T07:05:00Z"},"customer":{"avg_amount":50.0,"tx_count_24h":1,"known_merchants":["m4","m9"]},"merchant":{"id":"m4","mcc":"5812","avg_amount":38.0},"terminal":{"is_online":false,"card_present":true,"km_from_home":2.0},"last_transaction":{"timestamp":"2026-01-07T06:59:00Z","km_from_current":0.8}}
        """u8.ToArray(),
        """
        {"transaction":{"amount":3200.0,"installments":12,"requested_at":"2026-01-08T18:22:00Z"},"customer":{"avg_amount":210.0,"tx_count_24h":18,"known_merchants":["m3","m8"]},"merchant":{"id":"m5","mcc":"7801","avg_amount":1800.0},"terminal":{"is_online":false,"card_present":false,"km_from_home":640.0},"last_transaction":{"timestamp":"2026-01-08T17:00:00Z","km_from_current":310.0}}
        """u8.ToArray()
    ];
}
