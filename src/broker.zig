const std = @import("std");
const message_util = @import("message.zig");
const Message = message_util.Message;
const MessageType = message_util.MessageType;
const Io = std.Io;
const Allocator = std.mem.Allocator;
const net = Io.net;

const BROKER_PORT: u16 = 10000;

pub const Broker = struct {
    const Self = @This();

    pub fn init() Self {
        return Self{};
    }

    /// Main function to start an admin server and wait for a message
    pub fn startBrokerServer(self: *Self, io: Io) !void {
        // Shortcut for socket + bind in network using IPv4
        const addr = try net.IpAddress.parse("127.0.0.1", BROKER_PORT);
        var server = try addr.listen(io, .{ .mode = .stream, .protocol = .tcp, .reuse_address = true });
        while (true) {
            const stream = try server.accept(io); // Blocking until accepted

            // On accept, create internal buffers and readers
            var stream_read_buff: [1024]u8 = undefined;
            var stream_write_buff: [1024]u8 = undefined;
            var stream_rd = stream.reader(io, &stream_read_buff);
            var stream_wr = stream.writer(io, &stream_write_buff);

            // Read and process message
            if (try message_util.readMessageFromStream(&stream_rd)) |message| {
                if (try self.processBrokerMessage(io, message)) |response_message| {
                    try message_util.writeMessageToStream(&stream_wr, response_message);
                } else {
                    std.debug.print("Unsupported message type\n", .{});
                }
            }

            // Close the stream after
            stream.close(io);
        }
    }

    /// Parse a message sent to the admin process and call the correct processing function
    fn processBrokerMessage(self: *Self, io: Io, message: Message) !?Message {
        switch (message) {
            MessageType.ECHO => |echo_message| {
                const response_data = try self.processEchoMessage(echo_message);
                return message_util.Message{
                    .R_ECHO = response_data,
                };
            },
            MessageType.P_REG => |producer_register_message| {
                const response = try self.processProducerRegisterMessage(io, producer_register_message);
                return message_util.Message{
                    .R_P_REG = response,
                };
            },
            else => {
                // TODO: Support other type of message.
                return null;
            },
        }
    }

    fn processEchoMessage(_: *Self, message: []u8) ![]u8 {
        const return_data = try std.fmt.allocPrint(std.heap.page_allocator, "I have received: {s}", .{message});
        return return_data;
    }

    // 0 is good, 1 is bad
    fn processProducerRegisterMessage(_: *Self, io: Io, port_str: []u8) !u8 {
        const port_int = std.fmt.parseInt(u16, port_str, 10) catch |err| {
            std.debug.print("Error parsing port from string, err = {any}", .{err});
            return 1;
        };
        // Connect to it concurrently and process
        _ = try io.concurrent(connectAndReceive, .{ io, port_int });
        return 0;
    }
};

fn connectAndReceive(io: Io, port: u16) !void {
    std.debug.print("Connecting to producer at port {}\n", .{port});
    const addr = try net.IpAddress.parse("127.0.0.1", port);
    const stream = try addr.connect(io, .{ .mode = .stream, .protocol = .tcp });
    // Read input from stdin and write to stream.
    var stream_read_buff: [1024]u8 = undefined;
    var stream_write_buff: [1024]u8 = undefined;
    var stream_rd = stream.reader(io, &stream_read_buff);
    var stream_wr = stream.writer(io, &stream_write_buff);
    std.debug.print("Connected to producer port {}. Reading...\n", .{port});
    while (true) {
        if (try message_util.readMessageFromStream(&stream_rd)) |data| {
            std.debug.print("Received from producer: {s}\n", .{data.ECHO});
            const resp_data = try std.fmt.allocPrint(std.heap.page_allocator, "I have received {s}", .{data.ECHO});
            try message_util.writeMessageToStream(&stream_wr, message_util.Message{
                .R_ECHO = resp_data,
            });
        }
    }
}
