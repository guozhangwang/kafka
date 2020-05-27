package org.apache.kafka.raft;

public class LogAppendInfo {

    public long firstOffset;

    public long lastOffset;

    public LogAppendInfo(long firstOffset, long lastOffset) {
        this.firstOffset = firstOffset;
        this.lastOffset = lastOffset;
    }
}
