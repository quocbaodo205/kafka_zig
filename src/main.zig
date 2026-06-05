const std = @import("std");
const builtin = @import("builtin");
const Io = std.Io;
const Allocator = std.mem.Allocator;
const net = Io.net;
const message_util = @import("message.zig");
const Producer = @import("producer.zig").Producer;
const Consumer = @import("consumer.zig").Consumer;
// Pick the io_uring-backed broker on Linux, fall back to the portable
// Io-based broker everywhere else.
const Broker = if (builtin.os.tag == .linux)
    @import("broker_iou.zig").Broker
else
    @import("broker.zig").Broker;

pub fn main(init: std.process.Init) !void {
    // This is appropriate for anything that lives as long as the process.
    const arena: std.mem.Allocator = init.arena.allocator();

    // Accessing command line arguments:
    const args = try init.minimal.args.toSlice(arena);
    for (args) |arg| {
        std.log.info("arg: {s}", .{arg});
    }

    // Use c allocator
    const gpa = std.heap.c_allocator;

    // In order to do I/O operations need an `Io` instance.
    const io = init.io;

    if (std.mem.eql(u8, args[1], "server")) {
        var broker = try Broker.init(io, gpa);
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
    } else if (std.mem.eql(u8, args[1], "bench")) {
        try bench(io);
    }
}

pub fn initProducerWithParams(port: u16, topic: u16) !void {
    const gpa = std.heap.smp_allocator;
    // Set up our I/O implementation.
    var threaded: std.Io.Threaded = .init(gpa, .{ .environ = .empty });
    defer threaded.deinit();
    const io = threaded.io();

    var p = Producer.init(port, topic);
    try p.startAndSimulateProducerServer(io);
}

pub fn initConsumerWithParams(port: u16, topic: u16, group: u16) !void {
    const gpa = std.heap.smp_allocator;
    // Set up our I/O implementation.
    var threaded: std.Io.Threaded = .init(gpa, .{ .environ = .empty });
    defer threaded.deinit();
    const io = threaded.io();
    var c = Consumer.init(port, topic, group);
    try c.startConsumerServer(io);
}

pub fn startBroker() void {
    const gpa = std.heap.c_allocator;
    // Set up our I/O implementation.
    var threaded: std.Io.Threaded = .init(gpa, .{ .environ = .empty });
    defer threaded.deinit();
    const io = threaded.io();

    var broker = Broker.init(io, gpa) catch |err| {
        std.debug.print("err = {any}", .{err});
        return;
    };
    broker.startBrokerServer(io) catch |err| {
        std.debug.print("err = {any}", .{err});
        return;
    };
}

pub fn bench(io: Io) !void {
    // Use c allocator
    const gpa = std.heap.c_allocator;
    var threaded: std.Io.Threaded = .init(gpa, .{ .environ = .empty });
    defer threaded.deinit();
    var broker_th = try std.Thread.spawn(.{}, startBroker, .{});
    try io.sleep(.fromSeconds(2), .awake);
    var producer1_th = try std.Thread.spawn(.{}, initProducerWithParams, .{ 50000, 1 });
    try io.sleep(.fromSeconds(1), .awake);
    var producer2_th = try std.Thread.spawn(.{}, initProducerWithParams, .{ 50001, 1 });
    try io.sleep(.fromSeconds(1), .awake);
    var producer3_th = try std.Thread.spawn(.{}, initProducerWithParams, .{ 50002, 2 });
    try io.sleep(.fromSeconds(1), .awake);
    var consumer1_th = try std.Thread.spawn(.{}, initConsumerWithParams, .{ 30000, 1, 1 });
    try io.sleep(.fromSeconds(1), .awake);
    var consumer2_th = try std.Thread.spawn(.{}, initConsumerWithParams, .{ 30001, 1, 1 });
    try io.sleep(.fromSeconds(1), .awake);
    var consumer3_th = try std.Thread.spawn(.{}, initConsumerWithParams, .{ 31000, 1, 2 });
    try io.sleep(.fromSeconds(1), .awake);
    var consumer4_th = try std.Thread.spawn(.{}, initConsumerWithParams, .{ 40000, 2, 1 });
    try io.sleep(.fromSeconds(1), .awake);

    broker_th.join();
    producer1_th.join();
    producer2_th.join();
    producer3_th.join();
    consumer1_th.join();
    consumer2_th.join();
    consumer3_th.join();
    consumer4_th.join();
}
