const std = @import("std");
const Io = std.Io;
const Allocator = std.mem.Allocator;
const net = Io.net;
const message_util = @import("message.zig");
const Producer = @import("producer.zig").Producer;
const Consumer = @import("consumer.zig").Consumer;
const Broker = @import("broker.zig").Broker;

const iou = std.os.linux.IoUring;

const UserData = struct {
    command: u32, // 1: accept, 2: read, 3: write
    fd: i32,
    // Other data
    other: u64,
};

pub fn uringTCPECHOServer(io: Io) !void {
    var gpa = std.heap.page_allocator;

    var ring = try iou.init(8, 0);
    // Shortcut for socket + bind in network using IPv4
    const addr = try net.IpAddress.parse("127.0.0.1", 10000);
    // listen
    const server = try addr.listen(io, .{ .mode = .stream, .protocol = .tcp, .reuse_address = true });
    std.debug.print("Server waiting for accept...\n", .{});
    const fd = server.socket.handle;
    // Accept (need listen fd)
    var addrx: std.posix.sockaddr = undefined;
    var addr_len: std.posix.socklen_t = @sizeOf(std.posix.sockaddr);
    _ = try ring.accept_multishot(0, fd, &addrx, &addr_len, 0);
    _ = try ring.submit(); // Submit

    // Buffer group and recv
    var buffer_group = try iou.BufferGroup.init(&ring, gpa, 10, 1024, 8);

    while (true) {
        var cqe = try ring.copy_cqe(); // Blocking!!
        // std.debug.print("cqe = {any}\n", .{cqe});
        if (cqe.err() == .SUCCESS) {
            if (cqe.user_data == 0) {
                // Response of accept
                const acc_fd = cqe.res;
                std.debug.print("Accept command found, fd = {}\n", .{acc_fd});
                // Create a user data to store fd and command type
                const x = try gpa.create(UserData);
                x.* = UserData{
                    .command = 1,
                    .fd = acc_fd,
                    .other = 0,
                };
                _ = try buffer_group.recv(@intFromPtr(x), acc_fd, 0);
                _ = try ring.submit();
                // std.debug.print("Submit recv for fd = {}\n", .{acc_fd});
            } else {
                // Is a pointer to UserData
                const user_data_ptr: *UserData = @ptrFromInt(cqe.user_data);
                // std.debug.print("Got command data, struct = {any}", .{user_data_ptr.*});
                if (user_data_ptr.command == 1) {
                    // Resp of function recv. Get the buffer
                    const buf = try buffer_group.get(cqe);
                    const num_read: usize = @intCast(cqe.res);
                    // Build back message.
                    std.debug.print("Data received: {s}\n", .{buf[0..num_read]});

                    // Allocate new memory region + copy out.
                    const resp = try std.fmt.allocPrint(gpa, "{c}{s}", .{ @as(u8, @intCast(num_read)), buf[0..num_read] });

                    const x = try gpa.create(UserData);
                    x.* = UserData{
                        .command = 2, // Send command
                        .fd = user_data_ptr.fd,
                        .other = 0,
                    };
                    _ = try ring.send(@intFromPtr(x), user_data_ptr.fd, resp, 0);
                    _ = try ring.submit();

                    // Put back the buffer
                    try buffer_group.put(cqe);
                } else if (user_data_ptr.command == 2) {
                    // Resp of send.
                    const num_send = cqe.res;
                    if (num_send > 0) {
                        // Success, should close.
                        const x = try gpa.create(UserData);
                        x.* = UserData{
                            .command = 1, // Send command
                            .fd = user_data_ptr.fd,
                            .other = 0,
                        };
                        _ = try buffer_group.recv(@intFromPtr(x), user_data_ptr.fd, 0);
                        _ = try ring.submit();
                    }
                }
            }
        }
    }
}

pub fn client(io: Io, gpa: Allocator, identify: u32) !void {
    // Connect to broker process
    const addr = try net.IpAddress.parse("127.0.0.1", 10000);
    const stream = try addr.connect(io, .{ .mode = .stream, .protocol = .tcp });
    std.debug.print("Client connected!\n", .{});

    var stream_read_buff: [1024]u8 = undefined;
    var stream_write_buff: [1024]u8 = undefined;
    // Send register message to broker
    var stream_rd = stream.reader(io, &stream_read_buff);
    var stream_wr = stream.writer(io, &stream_write_buff);
    // Connect and write forever
    while (true) {
        try io.sleep(.fromSeconds(1), .awake);
        // Write some stuff
        const data = try std.fmt.allocPrint(gpa, "Hello from {}!", .{identify});
        try stream_wr.interface.writeAll(data);
        try stream_wr.interface.flush();
        std.debug.print("Client written: {s}\n", .{data});
        // // Read back echo
        const len = try stream_rd.interface.takeByte();
        // std.debug.print("incoming message len = {}\n", .{len});
        const resp = try stream_rd.interface.take(@intCast(len));
        std.debug.print("Recevied: {s}\n", .{resp});
    }
}

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
        var broker = try Broker.init(io, arena);
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
    } else if (std.mem.eql(u8, args[1], "echo_server")) {
        try uringTCPECHOServer(io);
    } else if (std.mem.eql(u8, args[1], "echo_client")) {
        const iden_int: u32 = try std.fmt.parseInt(u32, args[2], 10);
        try client(io, arena, iden_int);
    }
}
