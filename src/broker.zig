const std = @import("std");
const message_util = @import("message.zig");
const Message = message_util.Message;
const MessageType = message_util.MessageType;
const ProducerRegisterMessage = message_util.ProducerRegisterMessage;
const ConsumerRegisterMessage = message_util.ConsumerRegisterMessage;
const Topic = @import("topic.zig").Topic;
const CGroup = @import("cgroup.zig").CGroup;
const ConsumerConn = @import("cgroup.zig").ConsumerConn;
const Io = std.Io;
const Allocator = std.mem.Allocator;
const net = Io.net;

const BROKER_PORT: u16 = 10000;

pub const Broker = struct {
    const Self = @This();

    topics: std.ArrayList(*Topic),
    gpa: Allocator,

    pub fn init(allocator: Allocator) !Self {
        return Self{
            .topics = try std.ArrayList(*Topic).initCapacity(allocator, 10),
            .gpa = allocator,
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

    fn processProducerPCM(_: *Self, pcm: []const u8, topic: *Topic) !u8 {
        topic.mq.push(pcm);
        topic.mq.debug();
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
            MessageType.C_REG => |consumer_register_message| {
                const response = try self.processConsumerRegisterMessage(io, consumer_register_message);
                return message_util.Message{
                    .R_C_REG = response,
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
        std.debug.print("Broker received pRegMessage: port={}, topicID={}\n", .{ p_reg_message.port, p_reg_message.topicID });
        var topic: ?*Topic = null;
        for (self.topics.items) |tp| {
            if (tp.topicID == p_reg_message.topicID) {
                topic = tp;
                break;
            }
        }
        if (topic == null) {
            const tp = try self.gpa.create(Topic);
            tp.* = try Topic.init(p_reg_message.topicID, self.gpa);
            try self.topics.append(self.gpa, tp);
            topic = tp;
            _ = try io.concurrent(stopAndPop, .{ topic.?, io });
        }
        // Connect to it concurrently and process
        _ = try io.concurrent(connectAndReceiveProducer, .{ io, p_reg_message, self, topic.? });
        return 0;
    }

    fn processConsumerRegisterMessage(self: *Self, io: Io, c_reg_message: ConsumerRegisterMessage) !u8 {
        std.debug.print("Broker received cRegMessage: port={}, topicID={}, groupID={}\n", .{ c_reg_message.port, c_reg_message.topicID, c_reg_message.groupID });
        var topic: ?*Topic = null;
        for (self.topics.items) |tp| {
            if (tp.topicID == c_reg_message.topicID) {
                topic = tp;
                break;
            }
        }
        if (topic == null) {
            const tp = try self.gpa.create(Topic);
            tp.* = try Topic.init(c_reg_message.topicID, self.gpa);
            try self.topics.append(self.gpa, tp);
            topic = tp;
        }
        var cgroup: ?*CGroup = null;
        topic.?.lock.lockUncancelable(io);
        defer topic.?.lock.unlock(io);
        for (topic.?.cgroups.items) |cg| {
            if (cg.groupID == c_reg_message.groupID) {
                cgroup = cg;
                break;
            }
        }
        if (cgroup == null) {
            const cg = try self.gpa.create(CGroup);
            cg.* = try CGroup.init(self.gpa, c_reg_message.groupID);
            try topic.?.cgroups.append(self.gpa, cg);
            cgroup = cg;
            _ = try io.concurrent(startConsumerGroupConsumption, .{ topic.?, cgroup.?, io });
        }
        // Now connect to consumer and add to cgroup
        const addr = try net.IpAddress.parse("127.0.0.1", c_reg_message.port);
        const stream = try addr.connect(io, .{ .mode = .stream, .protocol = .tcp });
        std.debug.print("Connected to consumer at port {}\n", .{c_reg_message.port});
        const consumer = ConsumerConn{
            .status = true,
            .stream = stream,
        };
        try cgroup.?.consumers.append(self.gpa, consumer);
        return 0;
    }
};

fn stopAndPop(topic: *Topic, io: Io) void {
    while (true) {
        io.sleep(Io.Duration.fromSeconds(5), .boot) catch {};
        topic.lock.lockUncancelable(io);
        var min_offset: i32 = -1;
        for (topic.cgroups.items) |cg| {
            if (min_offset == -1) {
                min_offset = @intCast(cg.offset);
            } else {
                if (cg.offset < @as(u32, @intCast(min_offset))) {
                    min_offset = @intCast(cg.offset);
                }
            }
        }
        std.debug.print("Stop and pop run, minOffset = {}\n", .{min_offset});
        if (min_offset != -1) {
            for (topic.cgroups.items) |cg| {
                cg.lock.lockUncancelable(io);
                cg.offset -%= @intCast(min_offset);
            }
            for (0..@intCast(min_offset)) |_| {
                _ = topic.mq.pop();
            }
            for (topic.cgroups.items) |cg| {
                cg.lock.unlock(io);
            }
        }
        topic.lock.unlock(io);
    }
}

fn startConsumerGroupConsumption(topic: *Topic, cgroup: *CGroup, io: Io) void {
    std.debug.print("Starting consumer group process, topicID = {}, groupID = {}\n", .{ topic.topicID, cgroup.groupID });
    var stream_read_buff: [1024]u8 = undefined;
    var stream_write_buff: [1024]u8 = undefined;
    while (true) {
        cgroup.lock.lockUncancelable(io);
        const offset = cgroup.offset;
        // Take message from topic for consumption
        const pcm = topic.mq.peek(offset);
        if (pcm == null) {
            cgroup.lock.unlock(io);
            continue;
        }

        for (cgroup.consumers.items) |*consumer| {
            if (consumer.status) {
                var stream_rd = consumer.stream.reader(io, &stream_read_buff);
                var stream_wr = consumer.stream.writer(io, &stream_write_buff);
                // Write PCM message to ready consumer
                consumer.status = false;
                message_util.writeMessageToStream(&stream_wr, Message{
                    .PCM = pcm.?,
                }) catch |err| {
                    std.debug.print("Error writing message to consumer: {}\n", .{err});
                    consumer.status = false;

                    continue;
                };

                // Read ack
                if (message_util.readMessageFromStream(&stream_rd) catch |err| {
                    std.debug.print("Error reading ack from consumer: {}\n", .{err});
                    continue;
                }) |parsed_message| {
                    switch (parsed_message) {
                        MessageType.R_PCM => {
                            consumer.status = true;
                        },
                        else => {},
                    }
                    // Increase offset on consumed
                    cgroup.offset += 1;
                }
            } else {
                std.debug.print("No consumer is ready, size = {}\n", .{cgroup.consumers.items.len});
            }
        }
        cgroup.lock.unlock(io);
    }
}

fn connectAndReceiveProducer(io: Io, p_reg_message: ProducerRegisterMessage, broker: *Broker, topic: *Topic) !void {
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
                    const resp = try broker.processProducerPCM(pcm, topic);
                    try message_util.writeMessageToStream(&stream_wr, message_util.Message{
                        .R_PCM = resp,
                    });
                },
                else => {},
            }
        }
    }
}
