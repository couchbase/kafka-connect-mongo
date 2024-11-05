/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.debezium.connector.jdbc;

import org.apache.kafka.connect.sink.SinkRecord;
import org.jetbrains.annotations.NotNull;

import io.debezium.connector.jdbc.dialect.DatabaseDialect;
import io.debezium.connector.jdbc.util.SinkRecordFactory;

class AbstractRecordBufferTest {

    protected DatabaseDialect dialect;

    protected @NotNull SinkRecordDescriptor createRecord(SinkRecord kafkaRecord, JdbcSinkConnectorConfig config) {
        return SinkRecordDescriptor.builder()
                .withSinkRecord(kafkaRecord)
                .withDialect(dialect)
                .withPrimaryKeyMode(config.getPrimaryKeyMode())
                .withPrimaryKeyFields(config.getPrimaryKeyFields())
                .withFieldFilter(config.getFieldFilter())
                .build();
    }

    protected @NotNull SinkRecordDescriptor createRecordNoPkFields(SinkRecordFactory factory, byte i, JdbcSinkConnectorConfig config) {
        return createRecord(factory.createRecord("topic", i), config);
    }

    protected @NotNull SinkRecordDescriptor createRecordPkFieldId(SinkRecordFactory factory, byte i, JdbcSinkConnectorConfig config) {
        return createRecord(factory.createRecord("topic", i), config);
    }

}