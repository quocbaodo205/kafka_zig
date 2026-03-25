const std = @import("std");
const Io = std.Io;
const net = Io.net;

const BROKER_PORT: u16 = 10000;

const MessageType = enum(u8) {
    ECHO = 1,
    // Other
};

// Message is a tagged union
const Message = union(MessageType) {
    ECHO: []u8,
};

pub fn readFromStream(stream_rd: *net.Stream.Reader) !?[]u8 {
    const header = try stream_rd.interface.takeByte();
    if (header != 0) {
        const data = try stream_rd.interface.take(header);
        return data;
    }
    return null;
}

pub fn writeToStream(stream_wr: *net.Stream.Writer, data: []u8) !void {
    const new_data = try std.fmt.allocPrint(std.heap.page_allocator, "Received from client: {s}\n", .{data});
    try stream_wr.interface.writeByte(@as(u8, @intCast(new_data.len)));
    try stream_wr.interface.writeAll(new_data);
    try stream_wr.interface.flush();
}

pub const Broker = struct {
    const Self = @This();

    pub fn init() Self {
        return Self{};
    }

    pub fn startBrokerServer(self: *Self, io: Io) !void {
        const addr = try Io.net.IpAddress.parse("127.0.0.1", BROKER_PORT);
        var server = try addr.listen(io, .{ .mode = .stream, .protocol = .tcp, .reuse_address = true });

        var write_buf: [1024]u8 = undefined;
        var read_buf: [1024]u8 = undefined;
        // Event loop -> dispatch event
        while (true) {
            const stream = try server.accept(io); // Block until can
            var wr = stream.writer(io, &write_buf);
            var rd = stream.reader(io, &read_buf);

            // Read
            if (try readFromStream(&rd)) |stream_bytes| {
                // Process somehow...
                // Write back
                const parsed_message = self.parseBrokerMessage(stream_bytes);
                if (parsed_message) |message| {
                    if (try self.processBrokerMessage(message)) |response| {
                        try writeToStream(&wr, response);
                    }
                }
            }

            stream.close(io); // Close the stream to allow new connection.
        }
    }

    fn parseBrokerMessage(_: *Self, message: []u8) ?Message {
        switch (message[0]) {
            @intFromEnum(MessageType.ECHO) => {
                // Remove first byte (it's message type)
                return Message{ .ECHO = message[1..] };
            },
            else => {
                return null;
            },
        }
    }

    fn processBrokerMessage(_: *Self, message: Message) !?[]u8 {
        // Pattern matching. Exhausive.
        switch (message) {
            MessageType.ECHO => |echo_data| {
                const return_data = try std.fmt.allocPrint(std.heap.page_allocator, "I have received: {s}", .{echo_data});
                return return_data;
            },
        }
    }
};
