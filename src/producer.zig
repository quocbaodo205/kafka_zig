const std = @import("std");
const Io = std.Io;
const net = Io.net;
const message_util = @import("message.zig");
const ProducerRegisterMessage = message_util.ProducerRegisterMessage;

const BROKER_PORT: u16 = 10000;

pub const Producer = struct {
    const Self = @This();

    port: u16,
    topicID: u16,
    read_buffer: [1024]u8,
    write_buffer: [1024]u8,

    pub fn init(port: u16, topicID: u16) Self {
        return Self{
            .port = port,
            .topicID = topicID,
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
        const p_reg = ProducerRegisterMessage{ .port = self.port, .topicID = self.topicID };
        std.debug.print("Sent to server the port: {}, topic: {}\n", .{ self.port, self.topicID });
        try message_util.writeMessageToStream(&stream_wr, message_util.Message{
            .P_REG = p_reg,
        });
        // Try to read back the response from broker
        if (try message_util.readMessageFromStream(&stream_rd)) |res| {
            std.debug.print("Received ACK from server: {}\n", .{res.R_P_REG});
        }
        // Stream should be closed by the broker, no need to close ourselve.
    }

    pub fn startProducerServer(self: *Self, io: Io) !void {
        // Create TCP server
        const addr = try net.IpAddress.parse("127.0.0.1", self.port);
        var server = try addr.listen(io, .{ .mode = .stream, .protocol = .tcp, .reuse_address = true });

        try self.sendPortDataToBroker(io);

        const stream = try server.accept(io); // Blocking until accepted

        // Read input from stdin and write to stream.
        var stdin_buf: [1024]u8 = undefined;

        var rd: Io.File.Reader = .init(.stdin(), io, &stdin_buf);
        var stream_rd = stream.reader(io, &self.read_buffer);
        var stream_wr = stream.writer(io, &self.write_buffer);
        while (true) {
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
            std.debug.print("Sent to broker: {s}\n", .{line});
            try message_util.writeMessageToStream(&stream_wr, message_util.Message{
                .PCM = line,
            });
            // Try to read back from the stream
            if (try message_util.readMessageFromStream(&stream_rd)) |data| {
                std.debug.print("Receive R_PCM from broker: {}\n", .{data.R_PCM});
            }
        }
    }
};
