using System.Buffers;
using Rinha.Core;

ApplyRuntimeTuning();

var builder = WebApplication.CreateSlimBuilder(args);
builder.Logging.ClearProviders();
builder.WebHost.ConfigureKestrel(options =>
{
    options.Limits.MaxRequestBodySize = 8192;
    options.Limits.MaxConcurrentConnections = 8192;
    options.AddServerHeader = false;
    options.AllowSynchronousIO = false;
    string? socketPath = Environment.GetEnvironmentVariable("RINHA_SOCKET");
    if (!string.IsNullOrWhiteSpace(socketPath))
    {
        Directory.CreateDirectory(Path.GetDirectoryName(socketPath)!);
        if (File.Exists(socketPath))
            File.Delete(socketPath);
        options.ListenUnixSocket(socketPath, listenOptions =>
        {
            listenOptions.Protocols = Microsoft.AspNetCore.Server.Kestrel.Core.HttpProtocols.Http1;
        });
    }
    else
    {
        options.ListenAnyIP(9999);
    }
});

builder.Services.AddSingleton<IFraudSearch>(_ => FraudSearchRuntime.CreateFromEnvironment());

var app = builder.Build();
Warmup(app.Services.GetRequiredService<IFraudSearch>());

app.MapGet("/ready", static async (HttpContext context) =>
{
    ReadOnlyMemory<byte> response = Responses.Ready;
    context.Response.StatusCode = StatusCodes.Status200OK;
    context.Response.ContentType = "text/plain";
    context.Response.ContentLength = response.Length;
    await context.Response.Body.WriteAsync(response, context.RequestAborted);
});

app.MapPost("/fraud-score", static async (HttpContext context, IFraudSearch fraudSearch) =>
{
    try
    {
        int length = (int)(context.Request.ContentLength ?? 0);
        System.IO.Pipelines.ReadResult result;
        while (true)
        {
            result = await context.Request.BodyReader.ReadAsync(context.RequestAborted);
            if (result.IsCompleted || result.Buffer.Length >= length)
                break;
            context.Request.BodyReader.AdvanceTo(result.Buffer.Start, result.Buffer.End);
        }

        var buffer = result.Buffer;
        ReadOnlySpan<byte> span = buffer.IsSingleSegment ? buffer.FirstSpan : buffer.ToArray();
        if (length > 0 && span.Length > length)
            span = span.Slice(0, length);

        int frauds = fraudSearch.PredictFraudCount(span);
        context.Request.BodyReader.AdvanceTo(buffer.End);

        ReadOnlyMemory<byte> response = Responses.ByFraudCount[frauds];
        context.Response.StatusCode = StatusCodes.Status200OK;
        context.Response.ContentType = "application/json";
        context.Response.ContentLength = response.Length;
        await context.Response.Body.WriteAsync(response, context.RequestAborted);
    }
    catch
    {
        ReadOnlyMemory<byte> response = Responses.ByFraudCount[0];
        context.Response.StatusCode = StatusCodes.Status200OK;
        context.Response.ContentType = "application/json";
        context.Response.ContentLength = response.Length;
        await context.Response.Body.WriteAsync(response, context.RequestAborted);
    }
});

app.Run();

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
        for (int i = 0; i < 8; i++)
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
