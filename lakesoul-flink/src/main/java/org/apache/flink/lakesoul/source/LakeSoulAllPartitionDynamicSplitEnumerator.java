// SPDX-FileCopyrightText: 2023 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

package org.apache.flink.lakesoul.source;

import com.dmetasoul.lakesoul.lakesoul.io.substrait.SubstraitUtil;
import com.dmetasoul.lakesoul.meta.DataFileInfo;
import com.dmetasoul.lakesoul.meta.DataOperation;
import com.dmetasoul.lakesoul.meta.MetaVersion;
import com.dmetasoul.lakesoul.meta.PgmqMessage;
import com.dmetasoul.lakesoul.meta.dao.PgmqDao;
import com.dmetasoul.lakesoul.meta.entity.PartitionInfo;
import com.dmetasoul.lakesoul.meta.entity.TableInfo;
import com.dmetasoul.lakesoul.meta.DBConnector;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.substrait.proto.Plan;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.core.fs.Path;
import org.apache.flink.lakesoul.tool.FlinkUtil;
import org.apache.flink.shaded.guava31.com.google.common.collect.Maps;
import org.apache.flink.shaded.guava31.com.google.common.collect.Sets;
import org.apache.flink.table.runtime.arrow.ArrowUtils;
import org.apache.flink.table.types.logical.RowType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.*;
import java.util.stream.Collectors;
import java.security.MessageDigest;
import java.nio.charset.StandardCharsets;


import static com.dmetasoul.lakesoul.meta.DBConfig.LAKESOUL_NON_PARTITION_TABLE_PART_DESC;


public class LakeSoulAllPartitionDynamicSplitEnumerator
        implements SplitEnumerator<LakeSoulPartitionSplit, LakeSoulPendingSplits> {
    private static final Logger LOG = LoggerFactory.getLogger(LakeSoulAllPartitionDynamicSplitEnumerator.class);

    private final SplitEnumeratorContext<LakeSoulPartitionSplit> context;

    private final LakeSoulDynSplitAssigner splitAssigner;
    private final long discoveryInterval;
    private final Map<String, Long> partitionLatestTimestamp;
    private final Set<Integer> taskIdsAwaitingSplit;
    private final Plan partitionFilters;
    private final List<String> partitionColumns;
    private final TableInfo tableInfo;
    private final PgmqDao pgmqDao = new PgmqDao();
    private boolean hasPerformedInitialSnapshot;
    private final ObjectMapper objectMapper = new ObjectMapper();
    private long lastReadMsgId;
    protected Schema partitionArrowSchema;
    String tableId;
    String fullTableName;
    private long startTime;
    private long nextStartTime;
    private int hashBucketNum = -1;

    public LakeSoulAllPartitionDynamicSplitEnumerator(SplitEnumeratorContext<LakeSoulPartitionSplit> context,
                                                      LakeSoulDynSplitAssigner splitAssigner, RowType rowType,
                                                      long discoveryInterval, long startTime, String tableId,
                                                      String hashBucketNum, List<String> partitionColumns,
                                                      Plan partitionFilters, boolean hasPerformedInitialSnapshot, long lastReadMsgId) {
        this.context = context;
        this.splitAssigner = splitAssigner;
        this.discoveryInterval = discoveryInterval;
        this.tableId = tableId;
        this.startTime = startTime;
        this.hashBucketNum = Integer.parseInt(hashBucketNum);
        this.taskIdsAwaitingSplit = Sets.newConcurrentHashSet();
        this.partitionLatestTimestamp = Maps.newConcurrentMap();
        this.partitionColumns = partitionColumns;
        this.hasPerformedInitialSnapshot = hasPerformedInitialSnapshot;
        this.lastReadMsgId = lastReadMsgId;

        Schema tableSchema = ArrowUtils.toArrowSchema(rowType);
        List<Field>
                partitionFields =
                partitionColumns.stream().map(tableSchema::findField).collect(Collectors.toList());

        this.partitionArrowSchema = new Schema(partitionFields);
        this.partitionFilters = partitionFilters;
        tableInfo = DataOperation.dbManager().getTableInfoByTableId(tableId);
        fullTableName = tableInfo.getTableNamespace() + "." + tableInfo.getTableName();
        LOG.info("Create Dyn enumerator for table name {}, tableId {}, context {}," +
                        " filter {}, interval {} lastReadMsgId {} ",
                fullTableName, tableId, System.identityHashCode(context),
                partitionFilters, discoveryInterval, lastReadMsgId);
    }

    @Override
    public void start() {
        context.callAsync(this::enumerateSplits, this::processDiscoveredSplits, 0, discoveryInterval);
    }

    @Override
    public synchronized void handleSplitRequest(int subtaskId, @Nullable String requesterHostname) {
        LOG.info("handleSplitRequest for {}, subTaskId {}, oid {}, tid {}",
                fullTableName, subtaskId, System.identityHashCode(this), Thread.currentThread().getId());
        if (!context.registeredReaders().containsKey(subtaskId)) {
            // reader failed between sending the request and now. skip this request.
            return;
        }
        int tasksSize = context.registeredReaders().size();
        if (tasksSize == 0) {
            LOG.info("handleSplitRequest: Task size is 0 for subtaskId {} for table {}", subtaskId, fullTableName);
            taskIdsAwaitingSplit.add(subtaskId);
            return;
        }
        Optional<LakeSoulPartitionSplit> nextSplit = this.splitAssigner.getNext(subtaskId, tasksSize);
        if (nextSplit.isPresent()) {
            context.assignSplit(nextSplit.get(), subtaskId);
            taskIdsAwaitingSplit.remove(subtaskId);
        } else {
            taskIdsAwaitingSplit.add(subtaskId);
        }
    }

    @Override
    public synchronized void addSplitsBack(List<LakeSoulPartitionSplit> splits, int subtaskId) {
        LOG.info("Add split back {}, for table {}, subTaskId {}, oid {}, tid {}",
                splits, fullTableName, subtaskId,
                System.identityHashCode(this),
                Thread.currentThread().getId());
        splitAssigner.addSplits(splits);
    }

    @Override
    public void addReader(int subtaskId) {
    }

    @Override
    public LakeSoulPendingSplits snapshotState(long checkpointId) throws Exception {
        List<LakeSoulPartitionSplit> remaining;
        synchronized (this) {
            remaining = splitAssigner.remainingSplits();
        }
        LakeSoulPendingSplits pendingSplits = new LakeSoulPendingSplits(
                remaining, this.nextStartTime, this.tableId,
                "", this.discoveryInterval, this.hashBucketNum,  this.hasPerformedInitialSnapshot, this.lastReadMsgId);
        LOG.info("LakeSoulAllPartitionDynamicSplitEnumerator" +
                        "snapshotState, table {}, chkId {}, splits {}, oid {}, tid {} lastReadMsgId {} ",
                fullTableName, checkpointId, pendingSplits,
                System.identityHashCode(this),
                Thread.currentThread().getId(),
                this.lastReadMsgId);
        return pendingSplits;
    }

    @Override
    public void close() throws IOException {

    }

    private synchronized void processDiscoveredSplits(
            Collection<LakeSoulPartitionSplit> splits, Throwable error) {
        if (error != null) {
            LOG.error("Failed to enumerate files for table {}", fullTableName, error);
            return;
        }
        int tasksSize = context.registeredReaders().size();
        LOG.info("Process discovered splits for table {}, {}, taskSize {}, oid {}, tid {}", splits,
                fullTableName, tasksSize, System.identityHashCode(this),
                Thread.currentThread().getId());
        this.splitAssigner.addSplits(splits);
        if (tasksSize == 0) {
            return;
        }
        Iterator<Integer> iter = taskIdsAwaitingSplit.iterator();
        while (iter.hasNext()) {
            int taskId = iter.next();
            if (!context.registeredReaders().containsKey(taskId)) {
                iter.remove();
                continue;
            }
            Optional<LakeSoulPartitionSplit> al = this.splitAssigner.getNext(taskId, tasksSize);
            if (al.isPresent()) {
                context.assignSplit(al.get(), taskId);
                iter.remove();
            }
        }
        LOG.info("Process discovered splits done for table {}, {}, oid {}, tid {}",
                fullTableName, splits,
                System.identityHashCode(this),
                Thread.currentThread().getId());
    }

    public Collection<LakeSoulPartitionSplit> enumerateSplits() {
        LOG.info("enumerateSplits begin for table {}, oid {}, tid {}",
            fullTableName, System.identityHashCode(this), Thread.currentThread().getId());
        
        if (!hasPerformedInitialSnapshot || partitionColumns.isEmpty()) {
            long s = System.currentTimeMillis();
	    Collection<LakeSoulPartitionSplit> splits = enumerateSplitsSnapshot();
	    long e = System.currentTimeMillis();
	    LOG.info("enumerateSplits enumerateSplitsSnapshot cost time {}", (e - s));

            this.hasPerformedInitialSnapshot = true;
            return splits;
        }

        return enumerateSplitsPGMQ();
    }

    private Collection<LakeSoulPartitionSplit> enumerateSplitsSnapshot() {
        long s = System.currentTimeMillis();
        List<PartitionInfo> allPartitionInfo;
    
        if (partitionColumns.isEmpty()) {
            allPartitionInfo = DataOperation.dbManager().getPartitionInfos(tableId,
                Collections.singletonList(LAKESOUL_NON_PARTITION_TABLE_PART_DESC));
        } else {
            allPartitionInfo = MetaVersion.getAllPartitionInfo(tableId);
        }
    
        long e = System.currentTimeMillis();
        LOG.info("Snapshot Scan: Table {} allPartitionInfo size={}, queryTime={}ms",
            fullTableName, allPartitionInfo.size(), e - s);

        List<PartitionInfo> filteredPartition = SubstraitUtil.applyPartitionFilters(
            allPartitionInfo, partitionArrowSchema, partitionFilters);

        return processPartitionInfos(filteredPartition);
    }

    private String md5(String input) {
        try {
            MessageDigest md = MessageDigest.getInstance("MD5");
            byte[] hashInBytes = md.digest(input.getBytes(StandardCharsets.UTF_8));
            StringBuilder sb = new StringBuilder();
            for (byte b : hashInBytes) {
                sb.append(String.format("%02x", b));
            }
            return sb.toString();
        } catch (Exception e) {
            throw new RuntimeException("MD5 algorithm not found", e);
        }
    }

    private Collection<LakeSoulPartitionSplit> enumerateSplitsPGMQ() {
        ArrayList<LakeSoulPartitionSplit> splits = new ArrayList<>();
        String queueName = "ls_" + md5(tableId);

        long currentOffset = this.lastReadMsgId;

        List<PgmqMessage> messages = pgmqDao.readMessagesFromId(queueName, currentOffset, 500);

        if (messages.isEmpty()) {
            return splits;
        }

        long maxId = currentOffset;

	LOG.info("PGMQ QueueName {} Get Message {})", queueName, messages.size());

        Map<String, Long> partitionUpdates = new HashMap<>();

        for (PgmqMessage msg : messages) {

            long currentMsgId = msg.getMsgId();
            maxId = Math.max(maxId, msg.getMsgId());

            try {
                JsonNode node = objectMapper.readTree(msg.getMessageBody());
                String pDesc = node.get("partition_desc").asText();
                long pTs = node.get("timestamp").asLong();
                
                partitionUpdates.merge(pDesc, pTs, Math::max);
		        LOG.info("PGMQ Message id {}", msg.getMsgId());
            } catch (Exception e) {
                LOG.error("PGMQ Parse Error for msg_id=" + currentMsgId, e);
            }
        }

        this.lastReadMsgId = maxId;

        LOG.info("PGMQ Scan: Found {} updates for table {} ", partitionUpdates.size(), fullTableName);

        for (Map.Entry<String, Long> entry : partitionUpdates.entrySet()) {
            String partitionDesc = entry.getKey();
            long pgmqTimestamp = entry.getValue();
	    Long lastTimestamp;
            synchronized (this) {		    
              lastTimestamp = partitionLatestTimestamp.get(partitionDesc);
	      LOG.info("PGMQ PartitionLatestTimestamp {} partitionDesc {}", partitionLatestTimestamp.containsKey(partitionDesc) ? "NotEmpty" :"Empty", partitionDesc);
	    }
            long start = (lastTimestamp != null) ? lastTimestamp : this.startTime;
            long end = pgmqTimestamp + 1;

            if (start >= end) {
	            LOG.info("PGMQ Skip Partition: {}. Local: {}, Remote: {}. (Duplicated) lastTimestamp value {}, startTime {} ", partitionDesc, start, end, 
				lastTimestamp != null ? lastTimestamp : "null", this.startTime );
		        continue;
	        }

	        LOG.info("PGMQ Processing: {} range [{}, {})", partitionDesc, start, end);

            DataFileInfo[] files = DataOperation.getIncrementalPartitionDataInfo(
                    tableId, partitionDesc, start, end, "incremental");
            
            if (files.length > 0) {
		        LOG.info("PGMQ Found {} incremental files for {}", files.length, partitionDesc);    
                splits.addAll(createSplitsFromDataInfos(files, partitionDesc));
            }
            
            synchronized (this) {
            	partitionLatestTimestamp.put(partitionDesc, end);
	    }
        }

        return splits;
    }
     
    private Collection<LakeSoulPartitionSplit> processPartitionInfos(List<PartitionInfo> partitionInfos) {
        ArrayList<LakeSoulPartitionSplit> splits = new ArrayList<>(16);
    
        for (PartitionInfo partitionInfo : partitionInfos) {
            String partitionDesc = partitionInfo.getPartitionDesc();
            long latestTimestamp = partitionInfo.getTimestamp() + 1;
            this.nextStartTime = Math.max(latestTimestamp, this.nextStartTime);

            Long lastTimestamp;
            synchronized (this) {
                lastTimestamp = partitionLatestTimestamp.get(partitionDesc);
            }
        
            DataFileInfo[] dataFileInfos;
            if (lastTimestamp != null) {
                if (lastTimestamp == latestTimestamp) {
                    continue;
                }
                dataFileInfos = DataOperation.getIncrementalPartitionDataInfo(
                    tableId, partitionDesc, lastTimestamp, latestTimestamp, "incremental");
            } else {
                dataFileInfos = DataOperation.getIncrementalPartitionDataInfo(
                    tableId, partitionDesc, startTime, latestTimestamp, "incremental");
            }
        
            if (dataFileInfos.length > 0) {
                splits.addAll(createSplitsFromDataInfos(dataFileInfos, partitionDesc));
            }
        
            synchronized (this) {
                partitionLatestTimestamp.put(partitionDesc, latestTimestamp);
            }
        }
        return splits;
    }


    private List<LakeSoulPartitionSplit> createSplitsFromDataInfos(DataFileInfo[] dataFileInfos, String partitionDesc) {
        List<LakeSoulPartitionSplit> result = new ArrayList<>();
        Map<String, Map<Integer, List<Path>>> splitByRangeAndHashPartition =
            FlinkUtil.splitDataInfosToRangeAndHashPartition(tableInfo, dataFileInfos);
            
        for (Map.Entry<String, Map<Integer, List<Path>>> entry : splitByRangeAndHashPartition.entrySet()) {
            for (Map.Entry<Integer, List<Path>> split : entry.getValue().entrySet()) {
                result.add(new LakeSoulPartitionSplit(
                    String.valueOf(split.hashCode()), 
                    split.getValue(),
                    0, 
                    split.getKey(), 
                    partitionDesc));
            }
        }
        return result;
    }

}
