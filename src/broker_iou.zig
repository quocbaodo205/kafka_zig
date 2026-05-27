const std = @import("std");
const message_util = @import("message.zig");
const Message = message_util.Message;
const MessageType = message_util.MessageType;
const Topic = @import("topic.zig").Topic;
const CGroup = @import("cgroup.zig").CGroup;
const ConsumerConn = @import("cgroup.zig").ConsumerConn;
const broker_producer = @import("broker_producer_iou.zig");
const broker_consumer = @import("broker_consumer_iou.zig");
const Io = std.Io;
const File = Io.File;
const Dir = Io.Dir;
const Allocator = std.mem.Allocator;
const net = Io.net;
const posix = std.posix;
const linux = std.os.linux;
const iou = linux.IoUring;

const BROKER_PORT: u16 = 10000;

pub const BrokerComponent = enum(u8) {
    BROKER = 1,
    PRODUCER = 2,
    CONSUMER = 3,
};

pub const BrokerTaskState = enum(u8) {
    RECV = 1,
    SEND = 2,
    CLOSE = 3,
    // Producer-register flow (io_uring driven)
    SOCKET = 4,
    CONNECT = 5,
    // Consumer flow: 1s timeout SQE armed when a consumer's partition queue
    // is empty. Re-tries `trySendNext` when the timer fires.
    WAIT = 6,
};

pub const BrokerUserData = struct {
    comp: BrokerComponent,
    state: BrokerTaskState,
    fd: i32,
    temp_buffer: ?[]u8, // Use to store any temp string data.
    other: u64 = 0,
    // Producer stuff
    topic: ?*Topic = null,
    addr_in: posix.sockaddr.in = undefined,
    // Consumer stuff
    consumer: ?*ConsumerConn = null,
    cgroup: ?*CGroup = null,
    // For wait
    ts: linux.kernel_timespec = undefined,
};

pub const Broker = struct {
    const Self = @This();

    topics: std.ArrayList(*Topic),
    gpa: Allocator,

    meta_file: File,

    // io_uring stuff
    ring: iou = undefined,
    buffer_group: iou.BufferGroup = undefined,

    pub fn init(io: Io, allocator: Allocator) !Self {
        const cwd = Dir.cwd();

        // Open or create broker metadata file
        const meta_file = blk: {
            const f = cwd.createFile(io, "broker_metadata.dat", .{ .read = true, .truncate = false }) catch |err| {
                if (err == error.PathAlreadyExists) {
                    break :blk try cwd.openFile(io, "broker_metadata.dat", .{ .mode = .read_write });
                } else {
                    return err;
                }
            };
            break :blk f;
        };

        // Ensure metadata file is at least 4 bytes (topic count as u32)
        const meta_len = try meta_file.length(io);
        if (meta_len < 4) {
            var buf: [4]u8 = .{0} ** 4;
            try meta_file.writePositionalAll(io, &buf, 0);
        }

        // Read topic count from metadata file
        var count_buf: [4]u8 = undefined;
        _ = try meta_file.readPositionalAll(io, &count_buf, 0);
        const topic_count = std.mem.readInt(u32, &count_buf, .big);

        var topics = try std.ArrayList(*Topic).initCapacity(allocator, 10);

        // Restore topics from metadata
        if (topic_count > 0) {
            for (0..topic_count) |i| {
                var topic_id_buf: [2]u8 = undefined;
                _ = try meta_file.readPositionalAll(io, &topic_id_buf, 4 + i * 2);
                const topic_id = std.mem.readInt(u16, &topic_id_buf, .big);
                const tp = try allocator.create(Topic);
                tp.* = try Topic.init(io, topic_id, allocator);
                try topics.append(allocator, tp);
            }
        }

        std.debug.print("debug metadata file name = broker_metadata.dat: topics = {d}\n", .{topic_count});

        return Self{
            .topics = topics,
            .gpa = allocator,
            .meta_file = meta_file,
        };
    }

    pub fn store(self: *Self, io: Io) !void {
        var count_buf: [4]u8 = undefined;
        std.mem.writeInt(u32, &count_buf, @intCast(self.topics.items.len), .big);
        try self.meta_file.writePositionalAll(io, &count_buf, 0);
        for (self.topics.items, 0..) |tp, i| {
            var topic_id_buf: [2]u8 = undefined;
            std.mem.writeInt(u16, &topic_id_buf, tp.topicID, .big);
            try self.meta_file.writePositionalAll(io, &topic_id_buf, 4 + i * 2);
        }
        std.debug.print("debug metadata file name = broker_metadata.dat: topics = {d}\n", .{self.topics.items.len});
    }

    /// Main function to start an admin server and wait for a message
    pub fn startBrokerServer(self: *Self, io: Io) !void {
        self.ring = try iou.init(1 << 5, 0);
        var cqes: [1 << 5]linux.io_uring_cqe = undefined;
        self.buffer_group = try iou.BufferGroup.init(&self.ring, self.gpa, 1, 1 << 10, 1 << 5);

        // Shortcut for socket + bind in network using IPv4
        const addr = try net.IpAddress.parse("127.0.0.1", BROKER_PORT);
        const server = try addr.listen(io, .{ .mode = .stream, .protocol = .tcp, .reuse_address = true });

        // accept setup.
        const fd = server.socket.handle;
        var addrx: std.posix.sockaddr = undefined;
        var addr_len: std.posix.socklen_t = @sizeOf(std.posix.sockaddr);
        _ = try self.ring.accept_multishot(0, fd, &addrx, &addr_len, 0);
        _ = try self.ring.submit(); // Submit

        while (true) {
            const num_recv = try self.ring.copy_cqes(&cqes, 1);
            for (cqes[0..num_recv]) |cqe| {
                const err = cqe.err();
                if (err != .SUCCESS and err != .TIME) {
                    std.debug.print("Err in broker, cqe = {any}, err = {any}\n", .{ cqe, err });
                    continue;
                }
                if (cqe.user_data == 0) {
                    // accept return: We can read / write ack with it.
                    try self.processBrokerAccept(cqe);
                } else {
                    // Specific data.
                    const ud: *BrokerUserData = @ptrFromInt(cqe.user_data);
                    switch (ud.comp) {
                        BrokerComponent.BROKER => {
                            switch (ud.state) {
                                BrokerTaskState.RECV => {
                                    try self.processBrokerRecv(io, ud, cqe);
                                },
                                BrokerTaskState.SEND => {
                                    try self.processBrokerSend(ud);
                                },
                                else => {
                                    // TODO: nothing...
                                },
                            }
                        },
                        BrokerComponent.PRODUCER => {
                            try broker_producer.dispatch(self, io, ud, cqe);
                        },
                        BrokerComponent.CONSUMER => {
                            try broker_consumer.dispatch(self, io, ud, cqe);
                        },
                    }
                }
            }
        }
    }

    pub fn processBrokerAccept(self: *Self, cqe: linux.io_uring_cqe) !void {
        const fd = cqe.res; // The fd for the accepted socket (read / write using it)
        // We can start reading from the socket to see what's going on.
        const ud: *BrokerUserData = try self.gpa.create(BrokerUserData);
        ud.* = BrokerUserData{
            .comp = BrokerComponent.BROKER,
            .state = BrokerTaskState.RECV,
            .fd = fd,
            .temp_buffer = null,
        };
        const ud_int = @intFromPtr(ud);
        _ = try self.buffer_group.recv_multishot(ud_int, fd, 0);
        _ = try self.ring.submit();
    }

    pub fn processBrokerRecv(self: *Self, io: Io, ud: *BrokerUserData, cqe: linux.io_uring_cqe) !void {
        const data_full = try self.buffer_group.get(cqe);
        if (ud.temp_buffer) |current_data| {
            // Memory is lost here...
            const old_buf = current_data;
            ud.temp_buffer = try std.mem.concat(self.gpa, u8, &.{ current_data, data_full });
            self.gpa.free(old_buf);
        } else {
            ud.temp_buffer = try std.mem.concat(self.gpa, u8, &.{ "", data_full });
        }
        try self.buffer_group.put(cqe); // Give it back cuz not needed anymore.
        // Check if we have recv all message from the socket
        if (cqe.flags & linux.IORING_CQE_F_SOCK_NONEMPTY > 0) {
            return;
        }
        // Full data received, turn into Message and process (ignore first byte)
        if (message_util.parseMessage(ud.temp_buffer.?[1..])) |message| {
            if (try self.processBrokerMessage(io, message)) |resp| {
                // Write it back to the same fd
                const res_arr = try message_util.messageToBuffer(self.gpa, resp);
                const new_ud: *BrokerUserData = try self.gpa.create(BrokerUserData);
                new_ud.* = BrokerUserData{
                    .comp = BrokerComponent.BROKER,
                    .state = BrokerTaskState.SEND,
                    .fd = ud.fd,
                    .temp_buffer = res_arr,
                };
                const new_ud_int = @intFromPtr(new_ud);
                _ = try self.ring.send(new_ud_int, new_ud.fd, res_arr, 0);
                _ = try self.ring.submit();
            }
        } else {
            std.debug.print("Failed to parse message from: {s}\n", .{ud.temp_buffer.?[1..]});
        }
        self.gpa.free(ud.temp_buffer.?);
        ud.temp_buffer = null;
    }

    pub fn processBrokerSend(self: *Self, ud: *BrokerUserData) !void {
        // After send, we can just close.
        self.gpa.free(ud.temp_buffer.?);
        const new_ud: *BrokerUserData = try self.gpa.create(BrokerUserData);
        new_ud.* = BrokerUserData{
            .comp = BrokerComponent.BROKER,
            .state = BrokerTaskState.CLOSE,
            .fd = ud.fd,
            .temp_buffer = null,
        };
        const new_ud_int = @intFromPtr(new_ud);
        _ = try self.ring.close(new_ud_int, new_ud.fd);
        _ = try self.ring.submit();
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
                const response = try broker_producer.processProducerRegisterMessage(self, io, producer_register_message);
                return message_util.Message{
                    .R_P_REG = response,
                };
            },
            MessageType.C_REG => |consumer_register_message| {
                const response = try broker_consumer.processConsumerRegisterMessage(self, io, consumer_register_message);
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
};
