const std = @import("std");
const message_util = @import("message.zig");
const Message = message_util.Message;
const MessageType = message_util.MessageType;
const ConsumerRegisterMessage = message_util.ConsumerRegisterMessage;
const Topic = @import("topic.zig").Topic;
const CGroup = @import("cgroup.zig").CGroup;
const ConsumerConn = @import("cgroup.zig").ConsumerConn;
const Partition = @import("partition.zig").Partition;
const broker_iou = @import("broker_iou.zig");
const Broker = broker_iou.Broker;
const BrokerComponent = broker_iou.BrokerComponent;
const BrokerTaskState = broker_iou.BrokerTaskState;
const BrokerUserData = broker_iou.BrokerUserData;
const Io = std.Io;
const net = Io.net;
const posix = std.posix;
const linux = std.os.linux;

pub fn dispatch(broker: *Broker, io: Io, ud: *BrokerUserData, cqe: linux.io_uring_cqe) !void {
    switch (ud.state) {
        BrokerTaskState.SOCKET => try processConsumerSocket(broker, ud, cqe),
        BrokerTaskState.CONNECT => try processConsumerConnect(broker, io, ud),
        BrokerTaskState.SEND => try processConsumerSend(broker, ud),
        BrokerTaskState.RECV => try processConsumerRecv(broker, io, ud, cqe),
        BrokerTaskState.WAIT => try processConsumerWait(broker, io, ud),
        else => {
            // TODO: nothing...
        },
    }
}

// 0 is good, 1 is bad
pub fn processConsumerRegisterMessage(broker: *Broker, io: Io, c_reg_message: ConsumerRegisterMessage) !u8 {
    std.debug.print("Broker received cRegMessage: port={}, topicID={}, groupID={}\n", .{
        c_reg_message.port, c_reg_message.topicID, c_reg_message.groupID,
    });

    // Find or create the topic.
    var topic: ?*Topic = null;
    for (broker.topics.items) |tp| {
        if (tp.topicID == c_reg_message.topicID) {
            topic = tp;
            break;
        }
    }
    if (topic == null) {
        const tp = try broker.gpa.create(Topic);
        tp.* = try Topic.init(io, c_reg_message.topicID, broker.gpa);
        try broker.topics.append(broker.gpa, tp);
        try broker.store(io);
        topic = tp;
    }

    // Find or create the cgroup inside the topic.
    var cgroup: ?*CGroup = null;
    for (topic.?.cgroups.items) |cg| {
        if (cg.groupID == c_reg_message.groupID) {
            cgroup = cg;
            break;
        }
    }
    if (cgroup == null) {
        const cg = try broker.gpa.create(CGroup);
        cg.* = try CGroup.init(io, broker.gpa, c_reg_message.topicID, c_reg_message.groupID);
        try topic.?.cgroups.append(broker.gpa, cg);
        try topic.?.store(io);
        cgroup = cg;
    }

    const consumer_ptr = try broker.gpa.create(ConsumerConn);
    consumer_ptr.* = ConsumerConn{ .status = true };
    try cgroup.?.consumer_conn.append(broker.gpa, consumer_ptr);
    if (cgroup.?.partitions.items.len < cgroup.?.consumer_conn.items.len) {
        const new_partition_id: u16 = @intCast(cgroup.?.partitions.items.len + 1);
        try cgroup.?.partitions.append(
            broker.gpa,
            try Partition.init(io, c_reg_message.topicID, c_reg_message.groupID, new_partition_id),
        );
        try cgroup.?.store(io);
    }
    consumer_ptr.partition_idx = cgroup.?.partitions.items.len - 1;

    const ip4 = try net.Ip4Address.parse("127.0.0.1", c_reg_message.port);
    const addr_in: posix.sockaddr.in = .{
        .port = std.mem.nativeToBig(u16, ip4.port),
        .addr = @bitCast(ip4.bytes),
    };

    const ud = try broker.gpa.create(BrokerUserData);
    ud.* = BrokerUserData{
        .comp = BrokerComponent.CONSUMER,
        .state = BrokerTaskState.SOCKET,
        .fd = -1,
        .temp_buffer = null,
        .addr_in = addr_in,
        .consumer = consumer_ptr,
        .cgroup = cgroup,
    };
    const ud_int = @intFromPtr(ud);
    _ = try broker.ring.socket(ud_int, posix.AF.INET, posix.SOCK.STREAM, posix.IPPROTO.TCP, 0);
    _ = try broker.ring.submit();
    return 0;
}

fn processConsumerSocket(broker: *Broker, ud: *BrokerUserData, cqe: linux.io_uring_cqe) !void {
    ud.fd = cqe.res;
    ud.state = BrokerTaskState.CONNECT;
    const ud_int = @intFromPtr(ud);
    _ = try broker.ring.connect(
        ud_int,
        ud.fd,
        @ptrCast(&ud.addr_in),
        @sizeOf(posix.sockaddr.in),
    );
    _ = try broker.ring.submit();
}

fn processConsumerConnect(broker: *Broker, io: Io, ud: *BrokerUserData) !void {
    const c = ud.consumer.?;
    c.fd = ud.fd;
    c.status = true;
    std.debug.print("Consumer connected on fd={} (partition_idx={})\n", .{ ud.fd, c.partition_idx });

    ud.state = BrokerTaskState.RECV;
    const ud_int = @intFromPtr(ud);
    _ = try broker.buffer_group.recv_multishot(ud_int, ud.fd, 0);
    _ = try broker.ring.submit();

    try trySendNext(broker, io, ud.cgroup.?, c);
}

fn processConsumerSend(broker: *Broker, ud: *BrokerUserData) !void {
    if (ud.temp_buffer) |buf| broker.gpa.free(buf);
    broker.gpa.destroy(ud);
}

fn processConsumerRecv(broker: *Broker, io: Io, ud: *BrokerUserData, cqe: linux.io_uring_cqe) !void {
    const c = ud.consumer.?;
    const cgroup = ud.cgroup.?;
    const data_full = try broker.buffer_group.get(cqe);
    if (ud.temp_buffer) |current_data| {
        const old_buf = current_data;
        ud.temp_buffer = try std.mem.concat(broker.gpa, u8, &.{ current_data, data_full });
        broker.gpa.free(old_buf);
    } else {
        ud.temp_buffer = try std.mem.concat(broker.gpa, u8, &.{ "", data_full });
    }
    try broker.buffer_group.put(cqe);
    if (cqe.flags & linux.IORING_CQE_F_SOCK_NONEMPTY > 0) {
        return;
    }

    if (message_util.parseMessage(ud.temp_buffer.?[1..])) |message| {
        switch (message) {
            MessageType.R_PCM => {
                c.status = true;
            },
            else => {
                std.debug.print("Consumer recv got non-R_PCM message: {any}\n", .{message});
                return;
            },
        }
    } else {
        std.debug.print("Failed to parse consumer message from: {s}\n", .{ud.temp_buffer.?[1..]});
        return;
    }

    try trySendNext(broker, io, cgroup, c);
    broker.gpa.free(ud.temp_buffer.?);
    ud.temp_buffer = null;
}

fn trySendNext(broker: *Broker, io: Io, cgroup: *CGroup, c: *ConsumerConn) !void {
    if (!c.status or c.fd < 0) return;
    const partition = &cgroup.partitions.items[c.partition_idx];
    if (partition.queue.pop(io)) |pcm| {
        c.status = false;
        const resp_buf = try message_util.messageToBuffer(broker.gpa, Message{ .PCM = pcm });
        const send_ud = try broker.gpa.create(BrokerUserData);
        send_ud.* = BrokerUserData{
            .comp = BrokerComponent.CONSUMER,
            .state = BrokerTaskState.SEND,
            .fd = c.fd,
            .temp_buffer = resp_buf,
        };
        _ = try broker.ring.send(@intFromPtr(send_ud), c.fd, resp_buf, 0);
        _ = try broker.ring.submit();
    } else {
        try sendWait(broker, cgroup, c);
    }
}

fn sendWait(broker: *Broker, cgroup: *CGroup, c: *ConsumerConn) !void {
    const wait_ud = try broker.gpa.create(BrokerUserData);
    wait_ud.* = BrokerUserData{
        .comp = BrokerComponent.CONSUMER,
        .state = BrokerTaskState.WAIT,
        .fd = c.fd,
        .temp_buffer = null,
        .consumer = c,
        .cgroup = cgroup,
        .ts = .{ .sec = 1, .nsec = 0 },
    };
    _ = try broker.ring.timeout(
        @intFromPtr(wait_ud),
        &wait_ud.ts,
        0,
        linux.IORING_TIMEOUT_ETIME_SUCCESS,
    );
    _ = try broker.ring.submit();
}

fn processConsumerWait(broker: *Broker, io: Io, ud: *BrokerUserData) !void {
    const c = ud.consumer.?;
    const cgroup = ud.cgroup.?;
    try trySendNext(broker, io, cgroup, c);
}
