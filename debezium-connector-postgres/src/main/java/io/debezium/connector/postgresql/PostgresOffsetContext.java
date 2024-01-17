/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.postgresql;

import java.sql.SQLException;
import java.time.Instant;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import com.google.protobuf.MapEntry;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.errors.ConnectException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.connector.SnapshotRecord;
import io.debezium.connector.postgresql.connection.Lsn;
import io.debezium.connector.postgresql.connection.PostgresConnection;
import io.debezium.connector.postgresql.connection.ReplicationMessage.Operation;
import io.debezium.connector.postgresql.spi.OffsetState;
import io.debezium.pipeline.CommonOffsetContext;
import io.debezium.pipeline.source.snapshot.incremental.IncrementalSnapshotContext;
import io.debezium.pipeline.source.snapshot.incremental.SignalBasedIncrementalSnapshotContext;
import io.debezium.pipeline.spi.OffsetContext;
import io.debezium.pipeline.txmetadata.TransactionContext;
import io.debezium.relational.TableId;
import io.debezium.spi.schema.DataCollectionId;
import io.debezium.time.Conversions;
import io.debezium.util.Clock;

public class PostgresOffsetContext extends CommonOffsetContext<SourceInfo> {
    private static final Logger LOGGER = LoggerFactory.getLogger(PostgresOffsetContext.class);

    public static final String LAST_COMPLETELY_PROCESSED_LSN_KEY = "lsn_proc";
    public static final String LAST_COMMIT_LSN_KEY = "lsn_commit";

    public static final String COMMIT_LSN_PREFIX = "__LSN_COMMIT__TABLES__";
    public static final String SNAPSHOT_PREFIX = "__SNAPSHOTTED__TABLES__";
    public static final String CONSISTENT_POINT_KEY = "consistent_point";

    private final Schema sourceInfoSchema;

    private Map<TableId,Lsn> lastCommitLsnMap;
    private Lsn minLastCommitLsn;

    private boolean lastSnapshotRecord;
    private Lsn lastCompletelyProcessedLsn;
    private Lsn streamingStoppingLsn = null;
    private final TransactionContext transactionContext;
    private final IncrementalSnapshotContext<TableId> incrementalSnapshotContext;

    private Lsn consistentPoint;

    private Set<TableId> tableIds;

    private PostgresOffsetContext(PostgresConnectorConfig connectorConfig, Lsn lsn, Lsn lastCompletelyProcessedLsn, Lsn lastCommitLsn, Long txId, Operation messageType,
                                  Instant time,
                                  boolean snapshot,
                                  boolean lastSnapshotRecord, TransactionContext transactionContext,
                                  IncrementalSnapshotContext<TableId> incrementalSnapshotContext,
                                  Set<TableId> tableIds,
                                  Map<TableId,Lsn> lastCommitLsnMap,
                                  Lsn consistentPoint
                                  ) {
        super(new SourceInfo(connectorConfig));

        this.lastCompletelyProcessedLsn = lastCompletelyProcessedLsn;
        sourceInfo.update(lsn, time, txId, sourceInfo.xmin(), null, messageType);
        sourceInfo.updateLastCommit(lastCommitLsn);
        sourceInfoSchema = sourceInfo.schema();

        this.lastSnapshotRecord = lastSnapshotRecord;
        if (this.lastSnapshotRecord) {
            postSnapshotCompletion();
        }
        else {
            sourceInfo.setSnapshot(snapshot ? SnapshotRecord.TRUE : SnapshotRecord.FALSE);
        }
        this.transactionContext = transactionContext;
        this.incrementalSnapshotContext = incrementalSnapshotContext;
        this.tableIds=tableIds;
        this.minLastCommitLsn=lastCommitLsn;
        this.lastCommitLsnMap=lastCommitLsnMap;
        this.consistentPoint=consistentPoint;
    }

    public void addTable(TableId tableId) {
        tableIds.add(tableId);
    }
    public boolean hasTable(TableId tableId) {
        return tableIds.contains(tableId);
    }

    @Override
    public Map<String, ?> getOffset() {
        Map<String, Object> result = new HashMap<>();
        if (sourceInfo.timestamp() != null) {
            result.put(SourceInfo.TIMESTAMP_USEC_KEY, Conversions.toEpochMicros(sourceInfo.timestamp()));
        }
        if (sourceInfo.txId() != null) {
            result.put(SourceInfo.TXID_KEY, sourceInfo.txId());
        }
        if (sourceInfo.lsn() != null) {
            result.put(SourceInfo.LSN_KEY, sourceInfo.lsn().asLong());
        }
        if (consistentPoint!=null) {
            result.put(CONSISTENT_POINT_KEY, consistentPoint.asLong());
        }
        if (sourceInfo.xmin() != null) {
            result.put(SourceInfo.XMIN_KEY, sourceInfo.xmin());
        }
        if (sourceInfo.isSnapshot()) {
            result.put(SourceInfo.SNAPSHOT_KEY, true);
            result.put(SourceInfo.LAST_SNAPSHOT_RECORD_KEY, lastSnapshotRecord);
        }
        if (lastCompletelyProcessedLsn != null) {
            result.put(LAST_COMPLETELY_PROCESSED_LSN_KEY, lastCompletelyProcessedLsn.asLong());
        }
        if (minLastCommitLsn != null) {
            result.put(LAST_COMMIT_LSN_KEY, minLastCommitLsn.asLong());
        }
        if (sourceInfo.messageType() != null) {
            result.put(SourceInfo.MSG_TYPE_KEY, sourceInfo.messageType().toString());
        }

        for(TableId tableId:tableIds) {
            result.put(SNAPSHOT_PREFIX+tableId.toString(),"true");
        }

        for(TableId tableId:lastCommitLsnMap.keySet()) {
            result.put(COMMIT_LSN_PREFIX+tableId.toString(),lastCommitLsnMap.get(tableId).asLong());
        }


        return sourceInfo.isSnapshot() ? result : incrementalSnapshotContext.store(transactionContext.store(result));
    }

    @Override
    public Schema getSourceInfoSchema() {
        return sourceInfoSchema;
    }

    @Override
    public boolean isSnapshotRunning() {
        return sourceInfo.isSnapshot();
    }

    @Override
    public void preSnapshotStart() {
        sourceInfo.setSnapshot(SnapshotRecord.TRUE);
        lastSnapshotRecord = false;
    }

    public void SnapshotCompletion(TableId tableId)
    {
        this.lastCommitLsnMap.put(tableId,lsn());
    }

    @Override
    public void preSnapshotCompletion() {
        lastSnapshotRecord = true;
    }

    public void updateWalPosition(Lsn lsn, Lsn lastCompletelyProcessedLsn, Instant commitTime, Long txId, Long xmin, TableId tableId, Operation messageType) {

        if(tableId!=null){
            this.lastCommitLsnMap.put(tableId,lsn);
            this.minLastCommitLsn=lsn;

            for(Lsn lsnTable:lastCommitLsnMap.values()){
                if(lsnTable.compareTo(minLastCommitLsn)==-1){
                    this.minLastCommitLsn=lsnTable;
                }
            }
        }

        sourceInfo.update(lsn, commitTime, txId, xmin, tableId, messageType);
    }

    /**
     * update wal position for lsn events that do not have an associated table or schema
     */
    public void updateWalPosition(Lsn lsn, Lsn lastCompletelyProcessedLsn, Instant commitTime, Long txId, Long xmin, Operation messageType) {
        updateWalPosition(lsn, lastCompletelyProcessedLsn, commitTime, txId, xmin, null, messageType);
    }

    public void updateCommitPosition(Lsn lsn, Lsn lastCompletelyProcessedLsn,TableId tableId) {
        this.lastCommitLsnMap.put(tableId,lsn);
        this.minLastCommitLsn=lsn;

        for(Lsn lsnTable:lastCommitLsnMap.values()){
            if(lsnTable.compareTo(minLastCommitLsn)==-1){
                this.minLastCommitLsn=lsnTable;
            }
        }
        sourceInfo.updateLastCommit(this.minLastCommitLsn);
    }

    boolean hasLastKnownPosition() {
        return sourceInfo.lsn() != null;
    }

    boolean hasCompletelyProcessedPosition() {
        return this.lastCompletelyProcessedLsn != null;
    }

    Lsn lsn() {
        return sourceInfo.lsn();
    }

    Lsn lastCompletelyProcessedLsn() {
        return lastCompletelyProcessedLsn;
    }
    Lsn getMinLastCommitLsn() {
        return minLastCommitLsn;
    }

    Lsn lastCommitLsn() {
        return minLastCommitLsn;
    }

    Operation lastProcessedMessageType() {
        return sourceInfo.messageType();
    }

    /**
     * Returns the LSN that the streaming phase should stream events up to or null if
     * a stopping point is not set. If set during the streaming phase, any event with
     * an LSN less than the stopping LSN will be processed and once the stopping LSN
     * is reached, the streaming phase will end. Useful for a pre-snapshot catch up
     * streaming phase.
     */
    Lsn getStreamingStoppingLsn() {
        return streamingStoppingLsn;
    }

    public void setStreamingStoppingLsn(Lsn streamingStoppingLsn) {
        this.streamingStoppingLsn = streamingStoppingLsn;
    }

    Long xmin() {
        return sourceInfo.xmin();
    }

    public static class Loader implements OffsetContext.Loader<PostgresOffsetContext> {

        private final PostgresConnectorConfig connectorConfig;

        public Loader(PostgresConnectorConfig connectorConfig) {
            this.connectorConfig = connectorConfig;
        }

        private Long readOptionalLong(Map<String, ?> offset, String key) {
            final Object obj = offset.get(key);
            return (obj == null) ? null : ((Number) obj).longValue();
        }

        @SuppressWarnings("unchecked")
        @Override
        public PostgresOffsetContext load(Map<String, ?> offset) {
            final Lsn lsn = Lsn.valueOf(readOptionalLong(offset, SourceInfo.LSN_KEY));
            final Lsn lastCompletelyProcessedLsn = Lsn.valueOf(readOptionalLong(offset, LAST_COMPLETELY_PROCESSED_LSN_KEY));
            Lsn lastCommitLsn = Lsn.valueOf(readOptionalLong(offset, LAST_COMMIT_LSN_KEY));
            if (lastCommitLsn == null) {
                lastCommitLsn = lastCompletelyProcessedLsn;
            }
            final Long txId = readOptionalLong(offset, SourceInfo.TXID_KEY);
            final String msgType = (String) offset.getOrDefault(SourceInfo.MSG_TYPE_KEY, null);
            final Operation messageType = msgType == null ? null : Operation.valueOf(msgType);
            final Instant useconds = Conversions.toInstantFromMicros((Long) ((Map<String, Object>) offset).getOrDefault(SourceInfo.TIMESTAMP_USEC_KEY, 0L));
            final boolean snapshot = (boolean) ((Map<String, Object>) offset).getOrDefault(SourceInfo.SNAPSHOT_KEY, Boolean.FALSE);
            final boolean lastSnapshotRecord = (boolean) ((Map<String, Object>) offset).getOrDefault(SourceInfo.LAST_SNAPSHOT_RECORD_KEY, Boolean.FALSE);
            final Lsn consistencyPoint = Lsn.valueOf(readOptionalLong(offset,CONSISTENT_POINT_KEY));

            Set<TableId> SnapSnotTables=new HashSet<>();
            Map<TableId,Lsn> LsnMap= new HashMap<>();

            for (String key:offset.keySet()){
                if(key.startsWith(SNAPSHOT_PREFIX))
                {
                    String tableString = key.substring(SNAPSHOT_PREFIX.length());
                    SnapSnotTables.add(TableId.parse(tableString));
                }
                if(key.startsWith(COMMIT_LSN_PREFIX))
                {
                    String tableString = key.substring(COMMIT_LSN_PREFIX.length());
                    TableId tableId=TableId.parse(tableString);
                    Lsn tableLsn = Lsn.valueOf(readOptionalLong(offset,key));
                    LsnMap.put(tableId,tableLsn);
                }
            }





            return new PostgresOffsetContext(connectorConfig, lsn, lastCompletelyProcessedLsn, lastCommitLsn, txId, messageType, useconds, snapshot, lastSnapshotRecord,
                    TransactionContext.load(offset), SignalBasedIncrementalSnapshotContext.load(offset, false),SnapSnotTables,LsnMap,consistencyPoint);
        }
    }

    @Override
    public String toString() {
        return "PostgresOffsetContext [sourceInfoSchema=" + sourceInfoSchema + ", sourceInfo=" + sourceInfo
                + ", lastSnapshotRecord=" + lastSnapshotRecord
                + ", lastCompletelyProcessedLsn=" + lastCompletelyProcessedLsn + ", minLastCommitLsn=" + minLastCommitLsn
                + ", streamingStoppingLsn=" + streamingStoppingLsn + ", transactionContext=" + transactionContext
                + ", incrementalSnapshotContext=" + incrementalSnapshotContext
                + ", LsnMap=" + lastCommitLsnMap
                + ", ConsistentPoint=" +consistentPoint
                + "]";
    }

    public Lsn getConsistentPoint() {
        return consistentPoint;
    }

    public Lsn getLsnOfTable(TableId tableId){
        if(!lastCommitLsnMap.containsKey(tableId)) return null;
        return lastCommitLsnMap.get(tableId);
    }

    public Map<TableId,Lsn> getLastCommitLsnMap(){
        return lastCommitLsnMap;
    }

    public static PostgresOffsetContext initialContext(PostgresConnectorConfig connectorConfig, PostgresConnection jdbcConnection, Clock clock, Lsn consistentPoint) {
        return initialContext(connectorConfig, jdbcConnection, clock, null, null,new HashMap<>(),consistentPoint);
    }

    public static PostgresOffsetContext initialContext(PostgresConnectorConfig connectorConfig, PostgresConnection jdbcConnection, Clock clock, Lsn lastCommitLsn,
                                                       Lsn lastCompletelyProcessedLsn,Map<TableId,Lsn> LsnMap,Lsn consistentPoint) {
        try {
            LOGGER.info("Creating initial offset context");
            final Lsn lsn = Lsn.valueOf(jdbcConnection.currentXLogLocation());
            final long txId = jdbcConnection.currentTransactionId().longValue();
            LOGGER.info("Read xlogStart at '{}' from transaction '{}'", lsn, txId);


            Set<TableId> allTableIds = jdbcConnection.getAllTableIds(connectorConfig.databaseName());
            Set<TableId> SnapshottedTablesIds = new HashSet<>();

            for (TableId tableId:allTableIds) {
                if(connectorConfig.getTableFilters().dataCollectionFilter().isIncluded(tableId)){
                    SnapshottedTablesIds.add(tableId);
                }
            }


            return new PostgresOffsetContext(
                    connectorConfig,
                    lsn,
                    lastCompletelyProcessedLsn,
                    lastCommitLsn,
                    txId,
                    null,
                    clock.currentTimeAsInstant(),
                    false,
                    false,
                    new TransactionContext(),
                    new SignalBasedIncrementalSnapshotContext<>(false),
                    SnapshottedTablesIds,
                    LsnMap,
                    consistentPoint
                    );
        }
        catch (SQLException e) {
            throw new ConnectException("Database processing error", e);
        }
    }

    public OffsetState asOffsetState() {
        return new OffsetState(
                sourceInfo.lsn(),
                sourceInfo.txId(),
                sourceInfo.xmin(),
                sourceInfo.timestamp(),
                sourceInfo.isSnapshot());
    }

    @Override
    public void event(DataCollectionId tableId, Instant instant) {
        sourceInfo.update(instant, (TableId) tableId);
    }

    @Override
    public TransactionContext getTransactionContext() {
        return transactionContext;
    }

    @Override
    public IncrementalSnapshotContext<?> getIncrementalSnapshotContext() {
        return incrementalSnapshotContext;
    }
}
