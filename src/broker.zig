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
        var stream_read_buff: [1024]u8 = undefined;
        var stream_write_buff: [1024]u8 = undefined;

        while (true) {
            const stream = try server.accept(io); // Blocking until accepted

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

    fn processProducerPCM(_: *Self, io: Io, pcm: []const u8, topic: *Topic) !u8 {
        // If there are no cgroups yet, store in topic's mq
        if (topic.cgroups.items.len == 0) {
            topic.mq.push(pcm);
            return 0;
        }

        for (topic.cgroups.items) |cg| {
            var min_size: u32 = 1000000;
            var target_partition_idx: ?usize = null;
            for (cg.partitions.items, 0..) |partition, idx| {
                const current_size = partition.queue.size();
                if (current_size < min_size) {
                    min_size = current_size;
                    target_partition_idx = idx;
                }
            }
            if (target_partition_idx != null) {
                const target_partition = &cg.partitions.items[target_partition_idx.?];
                try target_partition.lock.lock(io);
                defer target_partition.lock.unlock(io);

                // First, dump all messages from topic's mq to this partition
                while (topic.mq.pop()) |msg_from_mq| {
                    target_partition.queue.push(msg_from_mq);
                }

                target_partition.queue.push(pcm);
            }
        }

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
        try topic.?.lock.lock(io);
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
        }
        // Now connect to consumer and add to cgroup
        const addr = try net.IpAddress.parse("127.0.0.1", c_reg_message.port);
        const stream = try addr.connect(io, .{ .mode = .stream, .protocol = .tcp });
        std.debug.print("Connected to consumer at port {}\n", .{c_reg_message.port});
        const consumer_ptr = try self.gpa.create(ConsumerConn);
        consumer_ptr.* = ConsumerConn{
            .status = true,
            .stream = stream,
        };
        try cgroup.?.lock.lock(io);
        defer cgroup.?.lock.unlock(io);
        try cgroup.?.consumer_conn.append(self.gpa, consumer_ptr);
        if (cgroup.?.partitions.items.len < cgroup.?.consumer_conn.items.len) {
            try cgroup.?.partitions.append(self.gpa, @import("partition.zig").Partition.init());
        }
        std.debug.print("Starting readConsumerReadyAndSend for group {} partition {}\n", .{ cgroup.?.groupID, cgroup.?.partitions.items.len - 1 });
        _ = try io.concurrent(readConsumerReadyAndSend, .{ io, topic.?, cgroup.?, consumer_ptr, cgroup.?.partitions.items.len - 1 });
        return 0;
    }
};

fn readConsumerReadyAndSend(io: Io, _: *Topic, cgroup: *CGroup, consumer_conn: *ConsumerConn, partition_idx: usize) void {
    var stream_read_buff: [1024]u8 = undefined;
    var stream_write_buff: [1024]u8 = undefined;
    var stream_rd = consumer_conn.stream.reader(io, &stream_read_buff);
    var stream_wr = consumer_conn.stream.writer(io, &stream_write_buff);

    while (true) {
        // Read ack if not ready
        if (!consumer_conn.status) {
            if (message_util.readMessageFromStream(&stream_rd) catch |err| {
                std.debug.print("Error reading R_PCM from consumer: {}\n", .{err});
                break;
            }) |parsed_message| {
                switch (parsed_message) {
                    MessageType.R_PCM => {
                        consumer_conn.status = true;
                    },
                    else => {
                        std.debug.print("Parsed message not R_PCM: {any}\n", .{parsed_message});
                        break;
                    },
                }
            } else {
                break;
            }
        }

        // Try to pop a message from any partition in this cgroup
        const pcm = cgroup.partitions.items[partition_idx].queue.pop();
        if (pcm == null) {
            continue;
        }

        // Write PCM message to ready consumer
        consumer_conn.status = false;
        message_util.writeMessageToStream(&stream_wr, Message{
            .PCM = pcm.?,
        }) catch |err| {
            std.debug.print("Error writing message to consumer: {}\n", .{err});
            continue;
        };
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
                    const resp = try broker.processProducerPCM(io, pcm, topic);
                    try message_util.writeMessageToStream(&stream_wr, message_util.Message{
                        .R_PCM = resp,
                    });
                },
                else => {},
            }
        }
    }
}
