package io.debezium.connector.mongodb.smt;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.transforms.Transformation;
import org.bson.Document;

import java.util.Map;

/**
 * @param <R> A record containing object in bson format
 *            eg: <pre>
 *                      {@code {
 *              "myInt": 1,
 *              "myString": "some foo bla text",
 *              "myDouble": {
 *                "$numberDouble": "20.21"
 *              },
 *              "mySubDoc": {
 *                "A": {
 *                  "$binary": {
 *                    "base64": "S2Fma2Egcm9ja3Mh",
 *                    "subType": "00"
 *                  }
 *                },
 *                "B": {
 *                  "$date": {
 *                    "$numberLong": "1577863627000"
 *                  }
 *                },
 *                "C": {
 *                  "$numberDecimal": "12345.6789"
 *                }
 *              },
 *              "myArray": [
 *                {
 *                  "$binary": {
 *                    "base64": "S2Fma2Egcm9ja3Mh",
 *                    "subType": "00"
 *                  }
 *                },
 *                {
 *                  "$date": {
 *                    "$numberLong": "1577863627000"
 *                  }
 *                },
 *                {
 *                  "$numberDecimal": "12345.6789"
 *                }
 *              ],
 *              "myBytes": {
 *                "$binary": {
 *                  "base64": "S2Fma2Egcm9ja3Mh",
 *                  "subType": "00"
 *                }
 *              },
 *              "myDate": {
 *                "$date": {
 *                  "$numberLong": "1577863627000"
 *                }
 *              },
 *              "myDecimal": {
 *                "$numberDecimal": "12345.6789"
 *              }
 *            }
 *                  }
 *                      </pre>
 * @param: output
 * and the result will be
 * <pre>
 *           {@code {
 *   "myInt": 1,
 *   "myString": "some foo bla text",
 *   "myDouble": 20.21,
 *   "mySubDoc": {
 *     "A": "S2Fma2Egcm9ja3Mh",
 *     "B": "2020-01-01T07:27:07Z",
 *     "C": "12345.6789"
 *   },
 *   "myArray": [
 *     "S2Fma2Egcm9ja3Mh",
 *     "2020-01-01T07:27:07Z",
 *     "12345.6789"
 *   ],
 *   "myBytes": "S2Fma2Egcm9ja3Mh",
 *   "myDate": "2020-01-01T07:27:07Z",
 *   "myDecimal": "12345.6789"
 * }
 *           }
 * </pre>
 */
public class BsonToJsonConverter<R extends ConnectRecord<R>> implements Transformation<R> {

    public static Object bsonToJsonConverter(Object value) {
        final Object newValue;
        if (value instanceof Map) {
            Map<String, Object> docMap = (Map<String, Object>) value;
            newValue = docMap;
        } else if (value instanceof String) {
            String doc = (String) value;
            String transformedDoc = Document.parse(doc).toJson(SimplifiedJson.getJsonWriter());
            newValue = transformedDoc;
        } else {
            newValue = value;
        }
        return newValue;
    }

    @Override
    public R apply(R record) {
        Object value = operatingValue(record);
        if (value == null) {
            return record;
        }

        final Object newValue = bsonToJsonConverter(value);

        return record.newRecord(record.topic(), record.kafkaPartition(),
                record.keySchema(), record.key(),
                record.valueSchema(), newValue,
                record.timestamp());
    }

    @Override
    public ConfigDef config() {
        return new ConfigDef();
    }

    @Override
    public void close() {

    }

    @Override
    public void configure(Map<String, ?> map) {

    }

    private Object operatingValue(R record) {
        return record.value();
    }
}

