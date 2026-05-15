const Queue = @import("queue.zig").Queue;
const CGroup = @import("cgroup.zig").CGroup;
const std = @import("std");
const Io = std.Io;
const File = Io.File;
const Dir = Io.Dir;

pub const Topic = struct {
    topicID: u16,
    mq: Queue(255, 10000),
    cgroups: std.ArrayList(*CGroup),
    lock: Io.Mutex,

    meta_file: File,

    const Self = @This();

    pub fn init(io: Io, tid: u16, allocator: std.mem.Allocator) !Self {
        const meta_fname = try std.fmt.allocPrint(std.heap.page_allocator, "topic_metadata_{d}.dat", .{tid});
        defer std.heap.page_allocator.free(meta_fname);

        const cwd = Dir.cwd();

        // Open or create metadata file
        const meta_file = blk: {
            const f = cwd.createFile(io, meta_fname, .{ .read = true, .truncate = false }) catch |err| {
                if (err == error.PathAlreadyExists) {
                    break :blk try cwd.openFile(io, meta_fname, .{ .mode = .read_write });
                } else {
                    return err;
                }
            };
            break :blk f;
        };

        // Ensure metadata file is at least 4 bytes (cgroup count as u32)
        const meta_len = try meta_file.length(io);
        if (meta_len < 4) {
            var buf: [4]u8 = .{0} ** 4;
            try meta_file.writePositionalAll(io, &buf, 0);
        }

        // Read cgroup count from metadata file
        var count_buf: [4]u8 = undefined;
        _ = try meta_file.readPositionalAll(io, &count_buf, 0);
        const cgroup_count = std.mem.readInt(u32, &count_buf, .big);

        var cgroups = try std.ArrayList(*CGroup).initCapacity(allocator, 10);

        // Restore cgroups from metadata
        if (cgroup_count > 0) {
            for (0..cgroup_count) |i| {
                var group_id_buf: [2]u8 = undefined;
                _ = try meta_file.readPositionalAll(io, &group_id_buf, 4 + i * 2);
                const group_id = std.mem.readInt(u16, &group_id_buf, .big);
                const cg = try allocator.create(CGroup);
                cg.* = try CGroup.init(io, allocator, tid, group_id);
                try cgroups.append(allocator, cg);
            }
        }

        std.debug.print("debug metadata file name = topic_metadata_{d}.dat: cgroups = {d}\n", .{ tid, cgroup_count });

        return Self{
            .topicID = tid,
            .mq = try Queue(255, 10000).init(io, tid, 65535, 65535),
            .cgroups = cgroups,
            .lock = .{ .state = .init(.unlocked) },
            .meta_file = meta_file,
        };
    }

    pub fn store(self: *Self, io: Io) !void {
        var count_buf: [4]u8 = undefined;
        std.mem.writeInt(u32, &count_buf, @intCast(self.cgroups.items.len), .big);
        try self.meta_file.writePositionalAll(io, &count_buf, 0);
        for (self.cgroups.items, 0..) |cg, i| {
            var group_id_buf: [2]u8 = undefined;
            std.mem.writeInt(u16, &group_id_buf, cg.groupID, .big);
            try self.meta_file.writePositionalAll(io, &group_id_buf, 4 + i * 2);
        }
        std.debug.print("debug metadata file name = topic_metadata_{d}.dat: cgroups = {d}\n", .{ self.topicID, self.cgroups.items.len });
    }
};
