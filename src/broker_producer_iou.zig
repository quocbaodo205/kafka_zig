const std = @import("std");
const message_util = @import("message.zig");
const Message = message_util.Message;
const MessageType = message_util.MessageType;
const ProducerRegisterMessage = message_util.ProducerRegisterMessage;
const Topic = @import("topic.zig").Topic;
const broker_iou = @import("broker_iou.zig");
const broker_consumer = @import("broker_consumer_iou.zig");
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
        BrokerTaskState.SOCKET => try processProducerSocket(broker, ud, cqe),
        BrokerTaskState.CONNECT => try processProducerConnect(broker, ud),
        BrokerTaskState.RECV => try processProducerRecv(broker, io, ud, cqe),
        else => {
            // TODO: nothing...
        },
    }
}

// 0 is good, 1 is bad
pub fn processProducerRegisterMessage(broker: *Broker, io: Io, p_reg_message: ProducerRegisterMessage) !u8 {
    std.debug.print("Broker received pRegMessage: port={}, topicID={}\n", .{ p_reg_message.port, p_reg_message.topicID });
    var topic: ?*Topic = null;
    for (broker.topics.items) |tp| {
        if (tp.topicID == p_reg_message.topicID) {
            topic = tp;
            break;
        }
    }
    if (topic == null) {
        const tp = try broker.gpa.create(Topic);
        tp.* = try Topic.init(io, p_reg_message.topicID, broker.gpa);
        try broker.topics.append(broker.gpa, tp);
        try broker.store(io);
        topic = tp;
    }

    const ip4 = try net.Ip4Address.parse("127.0.0.1", p_reg_message.port);
    const addr_in: posix.sockaddr.in = .{
        .port = std.mem.nativeToBig(u16, ip4.port),
        .addr = @bitCast(ip4.bytes),
    };

    const ud = try broker.gpa.create(BrokerUserData);
    ud.* = BrokerUserData{
        .comp = BrokerComponent.PRODUCER,
        .state = BrokerTaskState.SOCKET,
        .fd = -1,
        .temp_buffer = null,
        .topic = topic,
        .addr_in = addr_in,
    };
    const ud_int = @intFromPtr(ud);
    _ = try broker.ring.socket(ud_int, posix.AF.INET, posix.SOCK.STREAM, posix.IPPROTO.TCP, 0);
    _ = try broker.ring.submit();
    return 0;
}

pub fn processProducerPCM(io: Io, pcm: []const u8, topic: *Topic) !u8 {
    // If there are no cgroups yet, store in topic's mq
    if (topic.cgroups.items.len == 0) {
        topic.mq.push(io, pcm);
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
        if (target_partition_idx) |idx| {
            const target_partition = &cg.partitions.items[idx];
            try target_partition.lock.lock(io);
            defer target_partition.lock.unlock(io);

            // First, dump all messages from topic's mq to this partition
            while (topic.mq.pop(io)) |msg_from_mq| {
                target_partition.queue.push(io, msg_from_mq);
            }

            target_partition.queue.push(io, pcm);
        }
    }

    return 0;
}

fn processProducerSocket(broker: *Broker, ud: *BrokerUserData, cqe: linux.io_uring_cqe) !void {
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

fn processProducerConnect(broker: *Broker, ud: *BrokerUserData) !void {
    std.debug.print("Producer connected on fd={}\n", .{ud.fd});
    ud.state = BrokerTaskState.RECV;
    const ud_int = @intFromPtr(ud);
    _ = try broker.buffer_group.recv_multishot(ud_int, ud.fd, 0);
    _ = try broker.ring.submit();
}

fn processProducerRecv(broker: *Broker, io: Io, ud: *BrokerUserData, cqe: linux.io_uring_cqe) !void {
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
            MessageType.PCM => |pcm| {
                const resp_byte = try processProducerPCM(io, pcm, ud.topic.?);
                const resp_buf = try message_util.messageToBuffer(
                    broker.gpa,
                    Message{ .R_PCM = resp_byte },
                );
                const send_ud: *BrokerUserData = try broker.gpa.create(BrokerUserData);
                send_ud.* = BrokerUserData{
                    .comp = BrokerComponent.PRODUCER,
                    .state = BrokerTaskState.SEND,
                    .fd = ud.fd,
                    .temp_buffer = resp_buf,
                };
                _ = try broker.ring.send(@intFromPtr(send_ud), send_ud.fd, resp_buf, 0);
                _ = try broker.ring.submit();
            },
            else => {
                std.debug.print("Producer recv got non-PCM message: {any}\n", .{message});
            },
        }
    } else {
        std.debug.print("Failed to parse producer message from: {s}\n", .{ud.temp_buffer.?});
    }
    broker.gpa.free(ud.temp_buffer.?);
    ud.temp_buffer = null;
}
