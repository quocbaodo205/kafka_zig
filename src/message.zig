/// Utility to read/write to message from a stream
const std = @import("std");
const Io = std.Io;
const net = Io.net;

pub const ProducerRegisterMessage = struct {
    port: u16,
    topicID: u16,

    pub fn fromByte(self: *ProducerRegisterMessage, stream_message: []const u8) void {
        // First 2 bytes: port
        // Next 2 bytes: topicID
        self.port = (@as(u16, stream_message[0]) << 8) | stream_message[1];
        self.topicID = (@as(u16, stream_message[2]) << 8) | stream_message[3];
    }

    pub fn toByte(self: ProducerRegisterMessage) [4]u8 {
        var data: [4]u8 = undefined;
        // First 2 bytes: port
        // Next 2 bytes: topicID
        data[0] = @intCast(self.port >> 8);
        data[1] = @intCast(self.port & 0xff);
        data[2] = @intCast(self.topicID >> 8);
        data[3] = @intCast(self.topicID & 0xff);
        return data;
    }
};

pub const ConsumerRegisterMessage = struct {
    port: u16,
    topicID: u16,
    groupID: u16,

    pub fn fromByte(self: *ConsumerRegisterMessage, stream_message: []const u8) void {
        // First 2 bytes: port
        // Next 2 bytes: topicID
        // Next 2 bytes: groupID
        self.port = (@as(u16, stream_message[0]) << 8) | stream_message[1];
        self.topicID = (@as(u16, stream_message[2]) << 8) | stream_message[3];
        self.groupID = (@as(u16, stream_message[4]) << 8) | stream_message[5];
    }

    pub fn toByte(self: ConsumerRegisterMessage) [6]u8 {
        var data: [6]u8 = undefined;
        // First 2 bytes: port
        // Next 2 bytes: topicID
        // Next 2 bytes: groupID
        data[0] = @intCast(self.port >> 8);
        data[1] = @intCast(self.port & 0xff);
        data[2] = @intCast(self.topicID >> 8);
        data[3] = @intCast(self.topicID & 0xff);
        data[4] = @intCast(self.groupID >> 8);
        data[5] = @intCast(self.groupID & 0xff);
        return data;
    }
};

pub const MessageType = enum(u8) {
    ECHO = 1,
    P_REG = 2, // Register a producer
    C_REG = 3, // Register a consumer
    PCM = 4, // Producer message
    // Return message start at 100
    R_ECHO = 101,
    R_P_REG = 102,
    R_C_REG = 103,
    R_PCM = 104,
};

pub const Message = union(MessageType) {
    ECHO: []u8,
    P_REG: ProducerRegisterMessage,
    C_REG: ConsumerRegisterMessage,
    PCM: []u8,
    R_ECHO: []u8, // Echo back the message
    R_P_REG: u8, // Just return a number as ack
    R_C_REG: u8,
    R_PCM: u8,
};

/// Parse the message with the following format:
/// - First byte is the message type
/// - The remaining is the message content.
fn parseMessage(message: []u8) ?Message {
    switch (message[0]) {
        @intFromEnum(MessageType.ECHO) => {
            return Message{ .ECHO = message[1..] };
        },
        @intFromEnum(MessageType.R_ECHO) => {
            return Message{ .R_ECHO = message[1..] };
        },
        @intFromEnum(MessageType.P_REG) => {
            var p = ProducerRegisterMessage{ .port = 0, .topicID = 0 };
            p.fromByte(message[1..]);
            return Message{ .P_REG = p };
        },
        @intFromEnum(MessageType.R_P_REG) => {
            return Message{ .R_P_REG = message[1] };
        },
        @intFromEnum(MessageType.C_REG) => {
            var p = ConsumerRegisterMessage{ .port = 0, .topicID = 0, .groupID = 0 };
            p.fromByte(message[1..]);
            return Message{ .C_REG = p };
        },
        @intFromEnum(MessageType.R_C_REG) => {
            return Message{ .R_C_REG = message[1] };
        },
        @intFromEnum(MessageType.PCM) => {
            return Message{ .PCM = message[1..] };
        },
        @intFromEnum(MessageType.R_PCM) => {
            return Message{ .R_PCM = message[1] };
        },
        else => {
            // Do nothing here
            return null;
        },
    }
}

fn readFromStream(stream_rd: *net.Stream.Reader) !?[]u8 {
    const header = try stream_rd.interface.takeByte();
    if (header != 0) {
        const data = try stream_rd.interface.take(header);
        return data;
    } else {
        return null;
    }
}

/// Read a message from the stream
pub fn readMessageFromStream(stream_rd: *net.Stream.Reader) !?Message {
    const data = try readFromStream(stream_rd);
    if (data) |m| {
        return parseMessage(m);
    } else {
        return null;
    }
}

fn writeDataToStreamWithType(stream_wr: *net.Stream.Writer, mtype: u8, data: []const u8) !void {
    try stream_wr.interface.writeByte(@intCast(data.len + 1)); // Send how many byte written
    try stream_wr.interface.writeByte(mtype); // Send the type
    try stream_wr.interface.writeAll(data);
    try stream_wr.interface.flush();
}

/// Write a message to the stream
pub fn writeMessageToStream(stream_wr: *net.Stream.Writer, message: Message) !void {
    switch (message) {
        MessageType.ECHO => |data| {
            try writeDataToStreamWithType(stream_wr, @intFromEnum(MessageType.ECHO), data);
        },
        MessageType.P_REG => |data| {
            const bytes = data.toByte();
            try writeDataToStreamWithType(stream_wr, @intFromEnum(MessageType.P_REG), &bytes);
        },
        MessageType.C_REG => |data| {
            const bytes = data.toByte();
            try writeDataToStreamWithType(stream_wr, @intFromEnum(MessageType.C_REG), &bytes);
        },
        MessageType.PCM => |data| {
            try writeDataToStreamWithType(stream_wr, @intFromEnum(MessageType.PCM), data);
        },
        MessageType.R_ECHO => |data| {
            try writeDataToStreamWithType(stream_wr, @intFromEnum(MessageType.R_ECHO), data);
        },
        MessageType.R_P_REG => |ack_byte| {
            var data: [1]u8 = [1]u8{ack_byte};
            try writeDataToStreamWithType(stream_wr, @intFromEnum(MessageType.R_P_REG), &data);
        },
        MessageType.R_C_REG => |ack_byte| {
            var data: [1]u8 = [1]u8{ack_byte};
            try writeDataToStreamWithType(stream_wr, @intFromEnum(MessageType.R_C_REG), &data);
        },
        MessageType.R_PCM => |ack_byte| {
            var data: [1]u8 = [1]u8{ack_byte};
            try writeDataToStreamWithType(stream_wr, @intFromEnum(MessageType.R_PCM), &data);
        },
    }
}
