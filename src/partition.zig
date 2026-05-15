const std = @import("std");
const Io = std.Io;
const Queue = @import("queue.zig").Queue;

pub const Partition = struct {
    queue: Queue(255, 10000),
    lock: Io.Mutex,

    pub fn init(io: Io, topic_id: u16, cgroup_id: u16, partition_id: u16) !Partition {
        return Partition{
            .queue = try Queue(255, 10000).init(io, topic_id, cgroup_id, partition_id),
            .lock = .{ .state = .init(.unlocked) },
        };
    }
};
