/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.sink.naming;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.data.Envelope;
import io.debezium.sink.SinkConnectorConfig;

/**
 * Default implementation of the {@link CollectionNamingStrategy} where the table name is driven
 * directly from the topic name, replacing any {@code dot} characters with {@code underscore}
 * and source field in topic.
 *
 * @author Chris Cranford
 * @author rk3rn3r
 */
public class DefaultCollectionNamingStrategy implements CollectionNamingStrategy {
    private static final Logger LOGGER = LoggerFactory.getLogger(DefaultCollectionNamingStrategy.class);

    private final Pattern sourcePattern = Pattern.compile("\\$\\{(source\\.)(.*?)}");

    @Override
    public String resolveCollectionName(SinkConnectorConfig config, SinkRecord record) {
        // Default behavior is to replace dots with underscores
        final String topicName = record.topic().replace(".", "_");
        String table = config.getCollectionNameFormat().replace("${topic}", topicName);

        table = resolveCollectionNameBySource(config, record, table);
        return table;
    }

    private String resolveCollectionNameBySource(SinkConnectorConfig config, SinkRecord record, String tableFormat) {
        String table = tableFormat;
        if (table.contains("${source.")) {
            if (isTombstone(record)) {
                LOGGER.warn(
                        "Ignore this record because it seems to be a tombstone that doesn't have source field, then cannot resolve table name in topic '{}', partition '{}', offset '{}'",
                        record.topic(), record.kafkaPartition(), record.kafkaOffset());
                return null;
            }

            try {
                Struct source = ((Struct) record.value()).getStruct(Envelope.FieldName.SOURCE);
                Matcher matcher = sourcePattern.matcher(table);
                while (matcher.find()) {
                    String target = matcher.group();
                    table = table.replace(target, source.getString(matcher.group(2)));
                }
            }
            catch (DataException e) {
                LOGGER.error("Failed to resolve table name with format '{}', check source field in topic '{}'", config.getCollectionNameFormat(), record.topic(), e);
                throw e;
            }
        }
        return table;
    }

    private boolean isTombstone(SinkRecord record) {
        return record.value() == null;
    }

}