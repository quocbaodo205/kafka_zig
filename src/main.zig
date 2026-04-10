const std = @import("std");
const Io = std.Io;
const Allocator = std.mem.Allocator;
const net = Io.net;
const message_util = @import("message.zig");
const Producer = @import("producer.zig").Producer;

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
        var broker = try Broker.init();
        try broker.startBrokerServer(io);
    } else if (std.mem.eql(u8, args[1], "producer")) {
        const port_int: u16 = try std.fmt.parseInt(u16, args[2], 10);
        const topic_int: u16 = try std.fmt.parseInt(u16, args[3], 10);
        var producer = Producer.init(port_int, topic_int);
        try producer.startProducerServer(io);
    } else {
        try clientConnectTCPAndEcho(io, 10000);
    }
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
    try message_util.writeMessageToStream(&stream_wr, message_util.Message{
        .ECHO = line,
    });
    // Try to read back from the stream
    if (try message_util.readMessageFromStream(&stream_rd)) |data| {
        std.debug.print("Received from server: {s}\n", .{data.R_ECHO});
    }
}
