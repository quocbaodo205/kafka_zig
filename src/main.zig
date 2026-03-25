const std = @import("std");
const Io = std.Io;
const Allocator = std.mem.Allocator;
const net = Io.net;

// Local import
const Broker = @import("broker.zig").Broker;

pub fn main(init: std.process.Init) !void {
    // This is appropriate for anything that lives as long as the process.
    const arena: std.mem.Allocator = init.arena.allocator();

    // Accessing command line arguments:
    const args = try init.minimal.args.toSlice(arena);
    for (args) |arg| {
        std.log.info("arg: {s}", .{arg});
    }

    // In order to do I/O operations need an `Io` instance.
    const io = init.io;

    if (std.mem.eql(u8, args[1], "server")) {
        var broker = Broker.init();
        try broker.startBrokerServer(io);
    } else {
        // Parse port at 2nd argument
        try clientConnectTCPAndEcho(io, 10000);
    }
}

pub fn writeEchoToStream(stream_wr: *net.Stream.Writer, data: []u8) !void {
    try stream_wr.interface.writeByte(@intCast(data.len + 1)); // Send how many byte written
    try stream_wr.interface.writeByte(1); // Send echo command
    try stream_wr.interface.writeAll(data);
    try stream_wr.interface.flush();
}

/// Test echo function
pub fn clientConnectTCPAndEcho(io: Io, port: u16) !void {
    std.debug.print("Start client: ...\n", .{});
    const addr = try net.IpAddress.parse("127.0.0.1", port);
    const stream = try addr.connect(io, .{ .mode = .stream, .protocol = .tcp });
    // Read input from stdin and write to stream.
    var stdin_buf: [1024]u8 = undefined;
    var stream_read_buff: [1024]u8 = undefined;
    var stream_write_buff: [1024]u8 = undefined;

    var rd: Io.File.Reader = .init(.stdin(), io, &stdin_buf);
    var stream_rd = stream.reader(io, &stream_read_buff);
    var stream_wr = stream.writer(io, &stream_write_buff);
    const line = rd.interface.takeSentinel('\n') catch |err| {
        switch (err) {
            error.EndOfStream => {
                // Do nothing here...
                return;
            },
            else => {
                return err;
            },
        }
    };
    std.debug.print("Sent to server: {s}\n", .{line});
    try writeEchoToStream(&stream_wr, line);
    // Try to read back from the stream
    const header = try stream_rd.interface.takeByte(); // TODO: End of stream error handling
    if (header != 0) {
        const data = try stream_rd.interface.take(header);
        std.debug.print("Received from server: {s}\n", .{data});
    }
}
