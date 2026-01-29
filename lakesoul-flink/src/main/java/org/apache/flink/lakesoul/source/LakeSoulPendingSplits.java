// SPDX-FileCopyrightText: 2023 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

package org.apache.flink.lakesoul.source;

import java.util.List;

public class LakeSoulPendingSplits {

    /**
     * Split to read for both batch and streaming
     */
    private final List<LakeSoulPartitionSplit> splits;

    /**
     * Already discovered latest version's timestamp
     * For streaming only
     */
    private final long lastReadTimestamp;

    private final String tableId;
    private final String parDesc;
    private final long discoverInterval;
    private final int hashBucketNum;
    private final boolean hasPerformedInitialSnapshot;
    private final long lastReadMsgId;

    public LakeSoulPendingSplits(List<LakeSoulPartitionSplit> splits, long lastReadTimestamp, String tableId, String parDesc, long discoverInterval, int hashBucketNum, boolean hasPerformedInitialSnapshot, long lastReadMsgId) {
        this.splits = splits;
        this.lastReadTimestamp = lastReadTimestamp;
        this.tableId = tableId;
        this.parDesc = parDesc;
        this.discoverInterval = discoverInterval;
        this.hashBucketNum = hashBucketNum;
        this.hasPerformedInitialSnapshot = hasPerformedInitialSnapshot;
        this.lastReadMsgId = lastReadMsgId;
    }

    public List<LakeSoulPartitionSplit> getSplits() {
        return splits;
    }

    public long getLastReadTimestamp() {
        return lastReadTimestamp;
    }

    public String getTableId() {
        return tableId;
    }

    public String getParDesc() {
        return parDesc;
    }

    public long getDiscoverInterval() {
        return discoverInterval;
    }

    public int getHashBucketNum() {
        return hashBucketNum;
    }

    public boolean getPerformedInitialSnapshot() {
        return hasPerformedInitialSnapshot;
    }

    public long getLastReadMsgId() {
        return lastReadMsgId;
    }

    @Override
    public String toString() {
        return "LakeSoulPendingSplits{" + "splits=" + splits + ", lastReadTimestamp=" + lastReadTimestamp +
                ", tableid='" + tableId + '\'' + ", parDesc='" + parDesc + '\'' + ", discoverInterval=" +
                discoverInterval + ", hashBucketNum=" + hashBucketNum + '}';
    }
}
