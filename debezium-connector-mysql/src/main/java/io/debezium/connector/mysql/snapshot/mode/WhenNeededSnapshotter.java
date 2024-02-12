/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mysql.snapshot.mode;

import java.sql.SQLException;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.DebeziumException;
import io.debezium.bean.StandardBeanNames;
import io.debezium.config.CommonConnectorConfig;
import io.debezium.connector.mysql.MySqlConnectorConfig;
import io.debezium.connector.mysql.MySqlOffsetContext;
import io.debezium.connector.mysql.strategy.AbstractConnectorConnection;
import io.debezium.connector.mysql.strategy.mariadb.MariaDbConnection;
import io.debezium.connector.mysql.strategy.mariadb.MariaDbConnectorAdapter;
import io.debezium.connector.mysql.strategy.mysql.MySqlConnection;
import io.debezium.jdbc.JdbcConnection;
import io.debezium.pipeline.spi.OffsetContext;
import io.debezium.pipeline.spi.Offsets;
import io.debezium.pipeline.spi.Partition;
import io.debezium.snapshot.mode.HistorizedSnapshotter;

public class WhenNeededSnapshotter extends HistorizedSnapshotter {

    private static final Logger LOGGER = LoggerFactory.getLogger(WhenNeededSnapshotter.class);

    @Override
    public String name() {
        return MySqlConnectorConfig.SnapshotMode.WHEN_NEEDED.getValue();
    }

    @Override
    public void configure(Map<String, ?> properties) {

    }

    @Override
    public void validate(boolean offsetContextExists, boolean isSnapshotInProgress) {

        final CommonConnectorConfig config = beanRegistry.lookupByName(StandardBeanNames.CONNECTOR_CONFIG, CommonConnectorConfig.class);
        final JdbcConnection connection = beanRegistry.lookupByName(StandardBeanNames.JDBC_CONNECTION, JdbcConnection.class);
        final Offsets<Partition, OffsetContext> offsets = beanRegistry.lookupByName(StandardBeanNames.OFFSETS, Offsets.class);
        final OffsetContext offset = offsets.getTheOnlyOffset();

        try {
            if (offsetContextExists && !isSnapshotInProgress) {
                // Check to see if the server still has those binlog coordinates ...
                if (!connection.isLogPositionAvailable(offset, config)) {
                    LOGGER.warn(
                            "The connector is trying to read log starting at '{}', but this is no longer available on the server. Forcing the snapshot execution as it is allowed by the configuration.",
                            getStoredLogPosition(offset));
                    offsets.resetOffset(offsets.getTheOnlyPartition());

                }
            }
        }
        catch (SQLException e) {
            throw new DebeziumException("Unable to get last available log position", e);
        }
    }

    private Object getStoredLogPosition(OffsetContext offset) {
        return ((MySqlOffsetContext) offset).getSource().binlogPosition();
    }

    @Override
    protected boolean shouldSnapshotWhenNoOffset() {
        return true;
    }

    @Override
    protected boolean shouldSnapshotSchemaWhenNoOffset() {
        return true;
    }

    @Override
    public boolean shouldStream() {
        return true;
    }

    @Override
    public boolean shouldSnapshotOnSchemaError() {
        return false;
    }

    @Override
    public boolean shouldSnapshotOnDataError() {
        return true;
    }

    private Class<? extends AbstractConnectorConnection> getConnectionClass(MySqlConnectorConfig config) {
        // TODO review this when MariaDB becomes a first class connector
        if (config.getConnectorAdapter() instanceof MariaDbConnectorAdapter) {
            return MariaDbConnection.class;
        }
        else {
            return MySqlConnection.class;
        }
    }
}