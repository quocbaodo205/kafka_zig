const std = @import("std");
const Io = std.Io;
const Queue = @import("queue.zig").Queue;

pub const Partition = struct {
    queue: Queue(255, 10000),
    lock: Io.Mutex,

    pub fn init() Partition {
        return Partition{
            .queue = Queue(255, 10000).init(),
            .lock = .{ .state = .init(.unlocked) },
        };
    }
};
