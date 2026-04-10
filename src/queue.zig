const std = @import("std");

pub fn Queue(comptime message_size: comptime_int, comptime queue_max_size: comptime_int) type {
    const total_size = message_size * queue_max_size;
    return struct {
        const Self = @This();

        under_arr: []u8,
        under_size: []u8,
        head: u32 = 0,
        tail: u32 = 0,

        pub fn init() Self {
            return Self{
                .under_arr = std.heap.page_allocator.alloc(u8, total_size) catch unreachable,
                .under_size = std.heap.page_allocator.alloc(u8, total_size) catch unreachable,
            };
        }

        // Assume data length <= message_size
        pub fn push(self: *Self, data: []const u8) void {
            @memcpy(self.under_arr[self.tail .. self.tail + data.len], data);
            self.under_size[self.tail] = @intCast(data.len);
            self.tail += message_size;
            self.tail %= total_size;
        }

        pub fn pop(self: *Self) []u8 {
            const len = self.under_size[self.head];
            const data = self.under_arr[self.head .. self.head + len];
            self.head += message_size;
            self.head %= total_size;
            return data;
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
    };
}
