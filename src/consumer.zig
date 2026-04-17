const std = @import("std");
const Io = std.Io;
const net = Io.net;
const message_util = @import("message.zig");
const ConsumerRegisterMessage = message_util.ConsumerRegisterMessage;

const BROKER_PORT: u16 = 10000;

pub const Consumer = struct {
    const Self = @This();

    port: u16,
    topicID: u16,
    groupID: u16,
    read_buffer: [1024]u8,
    write_buffer: [1024]u8,

    pub fn init(port: u16, topicID: u16, groupID: u16) Self {
        return Self{
            .port = port,
            .topicID = topicID,
            .groupID = groupID,
            .read_buffer = undefined,
            .write_buffer = undefined,
        };
    }

    pub fn sendPortDataToBroker(self: *Self, io: Io) !void {
        // Connect to broker process
        const addr = try net.IpAddress.parse("127.0.0.1", BROKER_PORT);
        const stream = try addr.connect(io, .{ .mode = .stream, .protocol = .tcp });

        // Send register message to broker
        var stream_rd = stream.reader(io, &self.read_buffer);
        var stream_wr = stream.writer(io, &self.write_buffer);
        const c_reg = ConsumerRegisterMessage{ .port = self.port, .topicID = self.topicID, .groupID = self.groupID };
        std.debug.print("Sent to server the port: {}, topic: {}, group: {}\n", .{ self.port, self.topicID, self.groupID });
        try message_util.writeMessageToStream(&stream_wr, message_util.Message{
            .C_REG = c_reg,
        });
        // Try to read back the response from broker
        if (try message_util.readMessageFromStream(&stream_rd)) |res| {
            std.debug.print("Received ACK from server: {}\n", .{res.R_C_REG});
        }
        // Stream should be closed by the broker, no need to close ourselves.
    }

    pub fn startConsumerServer(self: *Self, io: Io) !void {
        // Create TCP server
        const addr = try net.IpAddress.parse("127.0.0.1", self.port);
        var server = try addr.listen(io, .{ .mode = .stream, .protocol = .tcp, .reuse_address = true });

        try self.sendPortDataToBroker(io);

        const stream = try server.accept(io); // Blocking until accepted

        // Read input from stdin and write to stream.
        var stream_rd = stream.reader(io, &self.read_buffer);
        var stream_wr = stream.writer(io, &self.write_buffer);
        std.debug.print("Started consumer server, receiving...\n", .{});

        while (true) {
            // Read message to consume
            if (try message_util.readMessageFromStream(&stream_rd)) |message| {
                switch (message) {
                    message_util.MessageType.PCM => |pcm| {
                        std.debug.print("Receive PCM from broker: {s}\n", .{pcm});
                        // Sleep for 1 second like in Go example
                        io.sleep(Io.Duration.fromSeconds(1), .boot) catch {};
                        // Write R_PCM
                        const resp: u8 = 1;
                        try message_util.writeMessageToStream(&stream_wr, message_util.Message{
                            .R_PCM = resp,
                        });
                    },
                    else => {},
                }
            } else {
                break;
            }
        }
        stream.close(io);
    }
};
