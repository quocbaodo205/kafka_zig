const std = @import("std");
const Io = std.Io;
const net = Io.net;

pub const ConsumerConn = struct {
    status: bool,
    stream: net.Stream,
};

pub const CGroup = struct {
    const Self = @This();

    groupID: u16,
    offset: u32 = 0,
    consumers: std.ArrayList(ConsumerConn),
    lock: Io.Mutex,

    pub fn init(gpa: std.mem.Allocator, group_id: u16) !Self {
        return Self{
            .groupID = group_id,
            .consumers = try std.ArrayList(ConsumerConn).initCapacity(gpa, 10),
            .lock = .{ .state = .init(.unlocked) },
        };
    }
};
