const std = @import("std");
const Io = std.Io;

pub fn main(init: std.process.Init) !void {
    // Prints to stderr, unbuffered, ignoring potential errors.
    std.debug.print("All your {s} are belong to us.\n", .{"codebase"});

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
        try spawnServer(io);
    } else {
        // Parse port at 2nd argument
        const port_int = try std.fmt.parseInt(u16, args[2], 10);
        try clientConnect(io, port_int);
    }
}

pub fn spawnServer(io: Io) !void {
    var task1 = try io.concurrent(startServer, .{ io, @as(u16, 10001) });
    var task2 = try io.concurrent(startServer, .{ io, @as(u16, 10002) });
    try task1.await(io);
    try task2.await(io);
}

pub fn startServer(io: Io, port: u16) !void {
    const addr = try Io.net.IpAddress.parse("127.0.0.1", port);
    var server = try addr.listen(io, .{ .mode = .stream, .protocol = .tcp, .reuse_address = true });
    const stream = try server.accept(io); // Block until can

    while (true) {
        // Read
        var read_buf: [1024]u8 = undefined;
        var rd = stream.reader(io, &read_buf);

        const header = try rd.interface.takeByte();
        const data = try rd.interface.take(header);
        std.debug.print("Receive from client: {s}\n", .{data});
        try io.sleep(.fromSeconds(2), .awake);

        // Write
        var write_buf: [1024]u8 = undefined;
        var wr = stream.writer(io, &write_buf);
        // Create a new []u8
        const new_data = try std.fmt.allocPrint(std.heap.page_allocator, "Received from client: {s}\n", .{data});
        try wr.interface.writeByte(@as(u8, @intCast(new_data.len)));
        try wr.interface.writeAll(new_data);
        try wr.interface.flush();
    }

    stream.close(io);
}

pub fn clientConnect(io: Io, port: u16) !void {
    const addr = try Io.net.IpAddress.parse("127.0.0.1", port);
    const stream = try addr.connect(io, .{ .mode = .stream, .protocol = .tcp }); // Error if no port binded

    while (true) {
        // Read from stdin
        var stdin_buf: [1024]u8 = undefined;
        var stdin_rd: Io.File.Reader = .init(.stdin(), io, &stdin_buf);
        const line = try stdin_rd.interface.takeSentinel('\n');
        std.debug.print("Client send to server: {s}\n", .{line});

        // Write
        var write_buf: [1024]u8 = undefined;
        var wr = stream.writer(io, &write_buf);
        try wr.interface.writeByte(@as(u8, @intCast(line.len)));
        try wr.interface.writeAll(line);
        try wr.interface.flush();

        // Read
        var read_buf: [1024]u8 = undefined;
        var rd = stream.reader(io, &read_buf);

        const header = try rd.interface.takeByte();
        const data = try rd.interface.take(header);
        std.debug.print("Receive from server: {s}\n", .{data});
    }

    stream.close(io);
}
