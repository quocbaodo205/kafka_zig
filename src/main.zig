const std = @import("std");
const Io = std.Io;
const Allocator = std.mem.Allocator;
const net = Io.net;
const message_util = @import("message.zig");
const Producer = @import("producer.zig").Producer;
const Consumer = @import("consumer.zig").Consumer;

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
        var broker = try Broker.init(arena);
        try broker.startBrokerServer(io);
    } else if (std.mem.eql(u8, args[1], "producer")) {
        const port_int: u16 = try std.fmt.parseInt(u16, args[2], 10);
        const topic_int: u16 = try std.fmt.parseInt(u16, args[3], 10);
        var producer = Producer.init(port_int, topic_int);
        // try producer.startProducerServer(io);
        try producer.startAndSimulateProducerServer(io);
    } else if (std.mem.eql(u8, args[1], "consumer")) {
        const port_int: u16 = try std.fmt.parseInt(u16, args[2], 10);
        const topic_int: u16 = try std.fmt.parseInt(u16, args[3], 10);
        const group_int: u16 = try std.fmt.parseInt(u16, args[4], 10);
        var consumer = Consumer.init(port_int, topic_int, group_int);
        try consumer.startConsumerServer(io);
    }
}
