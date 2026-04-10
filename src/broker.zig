const std = @import("std");
const message_util = @import("message.zig");
const Message = message_util.Message;
const MessageType = message_util.MessageType;
const ProducerRegisterMessage = message_util.ProducerRegisterMessage;
const Topic = @import("topic.zig").Topic;
const Io = std.Io;
const Allocator = std.mem.Allocator;
const net = Io.net;

const BROKER_PORT: u16 = 10000;

pub const Broker = struct {
    const Self = @This();

    topics: std.ArrayList(Topic),

    pub fn init() !Self {
        return Self{
            .topics = try std.ArrayList(Topic).initCapacity(std.heap.page_allocator, 10),
        };
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

    fn processProducerPCM(self: *Self, pcm: []const u8, topic_idx: usize) !u8 {
        self.topics.items[topic_idx].mq.push(pcm);
        self.topics.items[topic_idx].mq.debug();
        return 0;
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

    fn processEchoMessage(_: *Self, message: []const u8) ![]u8 {
        const return_data = try std.fmt.allocPrint(std.heap.page_allocator, "I have received: {s}", .{message});
        return return_data;
    }

    // 0 is good, 1 is bad
    fn processProducerRegisterMessage(self: *Self, io: Io, p_reg_message: ProducerRegisterMessage) !u8 {
        std.debug.print("p = {any}\n", .{p_reg_message});
        var topic_idx: isize = -1;
        for (self.topics.items, 0..) |tp, idx| {
            if (tp.topicID == p_reg_message.topicID) {
                topic_idx = @intCast(idx);
                break;
            }
        }
        if (topic_idx == -1) {
            try self.topics.append(std.heap.page_allocator, Topic.init(p_reg_message.topicID));
            topic_idx = @intCast(self.topics.items.len - 1);
        }
        // Connect to it concurrently and process
        _ = try io.concurrent(connectAndReceive, .{ io, p_reg_message, self, @as(usize, @intCast(topic_idx)) });
        return 0;
    }
};

fn connectAndReceive(io: Io, p_reg_message: ProducerRegisterMessage, broker: *Broker, topic_idx: usize) !void {
    std.debug.print("Connecting to producer at port {}\n", .{p_reg_message.port});
    const addr = try net.IpAddress.parse("127.0.0.1", p_reg_message.port);
    const stream = try addr.connect(io, .{ .mode = .stream, .protocol = .tcp });
    // Read input from stdin and write to stream.
    var stream_read_buff: [1024]u8 = undefined;
    var stream_write_buff: [1024]u8 = undefined;
    var stream_rd = stream.reader(io, &stream_read_buff);
    var stream_wr = stream.writer(io, &stream_write_buff);
    std.debug.print("Connected to producer port {}. Reading...\n", .{p_reg_message.port});
    while (true) {
        if (try message_util.readMessageFromStream(&stream_rd)) |data| {
            switch (data) {
                MessageType.PCM => |pcm| {
                    const resp = try broker.processProducerPCM(pcm, topic_idx);
                    try message_util.writeMessageToStream(&stream_wr, message_util.Message{
                        .R_PCM = resp,
                    });
                },
                else => {},
            }
        }
    }
}
