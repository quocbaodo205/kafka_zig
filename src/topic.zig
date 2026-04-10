const Queue = @import("queue.zig").Queue;

pub const Topic = struct {
    topicID: u16,
    mq: Queue(255, 10000),

    const Self = @This();

    pub fn init(tid: u16) Self {
        return Self{
            .topicID = tid,
            .mq = Queue(255, 10000).init(),
        };
    }
};
