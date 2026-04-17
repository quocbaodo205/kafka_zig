const Queue = @import("queue.zig").Queue;
const CGroup = @import("cgroup.zig").CGroup;
const std = @import("std");
const Io = std.Io;

pub const Topic = struct {
    topicID: u16,
    mq: Queue(255, 10000),
    cgroups: std.ArrayList(*CGroup),
    lock: Io.Mutex,

    const Self = @This();

    pub fn init(tid: u16, allocator: std.mem.Allocator) !Self {
        return Self{
            .topicID = tid,
            .mq = Queue(255, 10000).init(),
            .cgroups = try std.ArrayList(*CGroup).initCapacity(allocator, 10),
            .lock = .{ .state = .init(.unlocked) },
        };
    }
};
