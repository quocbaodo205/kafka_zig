const std = @import("std");
const Io = std.Io;
const net = Io.net;

const Partition = @import("partition.zig").Partition;

pub const ConsumerConn = struct {
    status: bool,
    stream: net.Stream,
};

pub const CGroup = struct {
    const Self = @This();

    groupID: u16,
    lock: Io.Mutex,

    // Partition based cgroup
    // Basically, a consumer group have a bunch of queues
    partitions: std.ArrayList(Partition),
    consumer_conn: std.ArrayList(*ConsumerConn),

    pub fn init(gpa: std.mem.Allocator, group_id: u16) !Self {
        var cg = Self{
            .groupID = group_id,
            .lock = .{ .state = .init(.unlocked) },
            .partitions = try std.ArrayList(Partition).initCapacity(gpa, 10),
            .consumer_conn = try std.ArrayList(*ConsumerConn).initCapacity(gpa, 10),
        };
        // Make an empty one for first push
        try cg.partitions.append(gpa, Partition.init());
        return cg;
    }
};
