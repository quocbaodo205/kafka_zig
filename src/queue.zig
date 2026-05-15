const std = @import("std");
const Io = std.Io;
const File = Io.File;
const Dir = Io.Dir;
const MemoryMap = File.MemoryMap;

pub fn Queue(comptime message_size: comptime_int, comptime queue_max_size: comptime_int) type {
    const total_size = message_size * queue_max_size;
    return struct {
        const Self = @This();

        under_arr: []u8,
        under_size: []u8,
        head: u32 = 0,
        tail: u32 = 0,

        meta_file: File,
        arr_mm: MemoryMap,
        size_mm: MemoryMap,

        pub fn init(io: Io, topic_id: u16, cgroup_id: u16, partition_id: u16) !Self {
            const meta_fname = try std.fmt.allocPrint(std.heap.page_allocator, "partition_metadata_{d}_{d}_{d}.dat", .{ topic_id, cgroup_id, partition_id });
            defer std.heap.page_allocator.free(meta_fname);
            const arr_fname = try std.fmt.allocPrint(std.heap.page_allocator, "underArr_{d}_{d}_{d}.dat", .{ topic_id, cgroup_id, partition_id });
            defer std.heap.page_allocator.free(arr_fname);
            const size_fname = try std.fmt.allocPrint(std.heap.page_allocator, "underSize_{d}_{d}_{d}.dat", .{ topic_id, cgroup_id, partition_id });
            defer std.heap.page_allocator.free(size_fname);

            const cwd = Dir.cwd();

            // Open or create metadata file for head/tail
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
            // Ensure metadata file is at least 8 bytes (head + tail)
            const meta_len = try meta_file.length(io);
            if (meta_len < 8) {
                var buf: [8]u8 = .{0} ** 8;
                try meta_file.writePositionalAll(io, &buf, 0);
            }

            // Read head and tail from metadata file
            var head: u32 = 0;
            var tail: u32 = 0;
            var head_buf: [4]u8 = undefined;
            var tail_buf: [4]u8 = undefined;
            _ = try meta_file.readPositionalAll(io, &head_buf, 0);
            _ = try meta_file.readPositionalAll(io, &tail_buf, 4);
            head = std.mem.readInt(u32, &head_buf, .big);
            tail = std.mem.readInt(u32, &tail_buf, .big);

            // Open or create underArr file with mmap
            const arr_file = blk: {
                const f = cwd.openFile(io, arr_fname, .{ .mode = .read_write }) catch |err| {
                    if (err == error.FileNotFound) {
                        var f2 = try cwd.createFile(io, arr_fname, .{ .read = true, .truncate = true });
                        // Pre-allocate to total_size
                        try f2.setLength(io, total_size);
                        break :blk f2;
                    } else {
                        return err;
                    }
                };
                // File exists, ensure it's the right size
                const f_len = try f.length(io);
                if (f_len < total_size) {
                    try f.setLength(io, total_size);
                }
                break :blk f;
            };
            const arr_mm = try arr_file.createMemoryMap(io, .{ .len = total_size });

            // Open or create underSize file with mmap
            const size_file = blk: {
                const f = cwd.openFile(io, size_fname, .{ .mode = .read_write }) catch |err| {
                    if (err == error.FileNotFound) {
                        var f2 = try cwd.createFile(io, size_fname, .{ .read = true, .truncate = true });
                        try f2.setLength(io, total_size);
                        break :blk f2;
                    } else {
                        return err;
                    }
                };
                const f_len = try f.length(io);
                if (f_len < total_size) {
                    try f.setLength(io, total_size);
                }
                break :blk f;
            };
            const size_mm = try size_file.createMemoryMap(io, .{ .len = total_size });

            return Self{
                .under_arr = arr_mm.memory[0..total_size],
                .under_size = size_mm.memory[0..total_size],
                .head = head,
                .tail = tail,
                .meta_file = meta_file,
                .arr_mm = arr_mm,
                .size_mm = size_mm,
            };
        }

        fn writeU32(file: File, io: Io, offset: u64, value: u32) !void {
            var buf: [4]u8 = undefined;
            std.mem.writeInt(u32, &buf, value, .big);
            try file.writePositionalAll(io, &buf, offset);
        }

        fn debugMetadata(self: *Self, _: Io) void {
            std.debug.print("debug metadata file: head = {d}, tail = {d}\n", .{ self.head, self.tail });
        }

        // Assume data length <= message_size
        pub fn push(self: *Self, io: Io, data: []const u8) void {
            @memcpy(self.under_arr[self.tail .. self.tail + data.len], data);
            self.under_size[self.tail] = @intCast(data.len);
            self.tail += message_size;
            self.tail %= total_size;

            writeU32(self.meta_file, io, 4, self.tail) catch |err| {
                std.debug.print("Error writing tail: {}\n", .{err});
            };
            self.debugMetadata(io);
        }

        pub fn pop(self: *Self, io: Io) ?[]u8 {
            if (self.head == self.tail) {
                return null;
            }
            const len = self.under_size[self.head];
            const data = self.under_arr[self.head .. self.head + len];
            self.head += message_size;
            self.head %= total_size;

            writeU32(self.meta_file, io, 0, self.head) catch |err| {
                std.debug.print("Error writing head: {}\n", .{err});
            };
            self.debugMetadata(io);
            return data;
        }

        pub fn peek(self: *Self, offset: u32) ?[]u8 {
            if (self.head == self.tail) {
                return null;
            }
            var position: u32 = self.head +% (offset * message_size);
            position %= total_size;
            if (self.head < self.tail) {
                if (!(position >= self.head and position < self.tail)) {
                    return null;
                }
            } else {
                if (!(position >= self.head or position < self.tail)) {
                    return null;
                }
            }
            const len = self.under_size[position];
            const data = self.under_arr[position .. position + len];
            return data;
        }

        pub fn size(self: *const Self) u32 {
            if (self.tail >= self.head) {
                return (self.tail - self.head) / message_size;
            } else {
                return ((total_size - self.head) + self.tail) / message_size;
            }
        }

        pub fn debug(self: *Self) void {
            std.debug.print("Debug queue: \n", .{});
            var cur = self.head;
            while (true) {
                const len = self.under_size[cur];
                const data = self.under_arr[cur..][0..len];
                std.debug.print("{s}\n", .{data});
                cur += message_size;
                cur %= total_size;
                if (cur == self.tail) {
                    break;
                }
            }
        }

        pub fn deinit(self: *Self, io: Io) void {
            self.arr_mm.destroy(io);
            self.size_mm.destroy(io);
            self.meta_file.close(io);
        }
    };
}
