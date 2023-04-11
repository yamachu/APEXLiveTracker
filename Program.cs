using System.Net.WebSockets;
using System.Threading.Channels;

var builder = WebApplication.CreateBuilder(args);

// Add services to the container.

builder.Services.AddControllers();

var app = builder.Build();

app.UseWebSockets(new WebSocketOptions
{
    KeepAliveInterval = TimeSpan.FromSeconds(10)
});

app.Use(async (context, next) =>
{
    Console.WriteLine($"Routing: {context.Request.Path}: {context.TraceIdentifier}");
    if (context.Request.Path == "/")
    {
        if (context.WebSockets.IsWebSocketRequest)
        {
            Console.WriteLine($"WS: Connection established: {context.TraceIdentifier}");
            using var webSocket = await context.WebSockets.AcceptWebSocketAsync();
            var ch = Channel.CreateUnbounded<APEXLiveEvent>();

            var source = new CancellationTokenSource();
            var token = source.Token;

            var wsWatcher = Task.Run(() =>
            {
                while (webSocket.State == WebSocketState.Open) {}

                Console.WriteLine($"WS: Connection state changed: {webSocket.State}: {context.TraceIdentifier}");
                token.ThrowIfCancellationRequested();
            });

            var consumer = Task.Run(async () =>
            {
                while (await ch.Reader.WaitToReadAsync())
                {
                    var (Id, RawEvent) = await ch.Reader.ReadAsync();
                    // TODO: Write to db operation
                    // Don't parse here, only write operation!
                    var parsedEvent = Rtech.Liveapi.LiveAPIEvent.Parser.ParseFrom(RawEvent);
                    Console.WriteLine($"Sender: {Id}, Value: {parsedEvent}");
                }
            });

            var buffer = new byte[1024 * 4];
            var receiveResult = await webSocket.ReceiveAsync(
                new ArraySegment<byte>(buffer), token);

            while (!receiveResult.CloseStatus.HasValue)
            {
                var received = new ArraySegment<byte>(buffer, 0, receiveResult.Count).ToArray();
                await ch.Writer.WriteAsync(new APEXLiveEvent(context.TraceIdentifier, received));

                receiveResult = await webSocket.ReceiveAsync(
                    new ArraySegment<byte>(buffer), token);
            }

            await webSocket.CloseAsync(
                receiveResult.CloseStatus.Value,
                receiveResult.CloseStatusDescription,
                CancellationToken.None);
            ch.Writer.Complete();
            await consumer;
            await wsWatcher;
            Console.WriteLine($"WS: Disconnected: {context.TraceIdentifier}");
        }
        else
        {
            context.Response.StatusCode = StatusCodes.Status400BadRequest;
        }
    }
    else
    {
        await next(context);
    }
});

app.Run();

record APEXLiveEvent(string Id, byte[] RawEvent);
