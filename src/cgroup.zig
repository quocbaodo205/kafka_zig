const std = @import("std");
const Io = std.Io;
const File = Io.File;
const Dir = Io.Dir;
const net = Io.net;

const Partition = @import("partition.zig").Partition;

pub const ConsumerConn = struct {
    status: bool,
    // Used by the Io-based (non-Linux) broker. The io_uring broker leaves
    // this field as `undefined` and reads/writes via `fd` instead.
    stream: net.Stream = undefined,
    // io_uring broker only: raw socket fd for this consumer, plus the
    // index of the partition this consumer is assigned to within its cgroup.
    fd: i32 = -1,
    partition_idx: usize = 0,
};

pub const CGroup = struct {
    const Self = @This();

    groupID: u16,
    topicID: u16,
    lock: Io.Mutex,

    // Partition based cgroup
    // Basically, a consumer group have a bunch of queues
    partitions: std.ArrayList(Partition),
    consumer_conn: std.ArrayList(*ConsumerConn),

    meta_file: File,

    pub fn init(io: Io, gpa: std.mem.Allocator, topic_id: u16, group_id: u16) !Self {
        const meta_fname = try std.fmt.allocPrint(std.heap.page_allocator, "cgroup_metadata_{d}_{d}.dat", .{ topic_id, group_id });
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

        // Ensure metadata file is at least 4 bytes (partition count as u32)
        const meta_len = try meta_file.length(io);
        if (meta_len < 4) {
            var buf: [4]u8 = .{0} ** 4;
            try meta_file.writePositionalAll(io, &buf, 0);
        }

        // Read partition count from metadata file
        var count_buf: [4]u8 = undefined;
        _ = try meta_file.readPositionalAll(io, &count_buf, 0);
        const partition_count = std.mem.readInt(u32, &count_buf, .big);

        var cg = Self{
            .groupID = group_id,
            .topicID = topic_id,
            .lock = .{ .state = .init(.unlocked) },
            .partitions = try std.ArrayList(Partition).initCapacity(gpa, 10),
            .consumer_conn = try std.ArrayList(*ConsumerConn).initCapacity(gpa, 10),
            .meta_file = meta_file,
        };

        // Restore partitions from metadata
        if (partition_count > 0) {
            for (0..partition_count) |i| {
                try cg.partitions.append(gpa, try Partition.init(io, topic_id, group_id, @intCast(i + 1)));
            }
        } else {
            // Make an empty one for first push
            try cg.partitions.append(gpa, try Partition.init(io, topic_id, group_id, 1));
            try cg.store(io);
        }
        return cg;
    }

    pub fn store(self: *Self, io: Io) !void {
        var buf: [4]u8 = undefined;
        std.mem.writeInt(u32, &buf, @intCast(self.partitions.items.len), .big);
        try self.meta_file.writePositionalAll(io, &buf, 0);
        std.debug.print("debug metadata file name = cgroup_metadata_{d}_{d}.dat: sz = {d}\n", .{ self.topicID, self.groupID, self.partitions.items.len });
    }
};
