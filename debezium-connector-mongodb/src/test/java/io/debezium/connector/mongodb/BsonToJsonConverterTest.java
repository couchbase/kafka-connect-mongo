package io.debezium.connector.mongodb;

import io.debezium.connector.mongodb.smt.BsonToJsonConverter;
import org.junit.Test;

import static java.lang.String.format;
import static org.junit.Assert.assertEquals;

public class BsonToJsonConverterTest {

    @Test
    public void convertFromBsonToJson() {
        String documentString =
                "{'myInt': %s, "
                        + "'myString': 'some foo bla text', "
                        + "'myDouble': {'$numberDouble': '20.21'}, "
                        + "'mySubDoc': {'A': {'$binary': {'base64': 'S2Fma2Egcm9ja3Mh', 'subType': '00'}}, "
                        + "  'B': {'$date': {'$numberLong': '1577863627000'}}, 'C': {'$numberDecimal': '12345.6789'}}, "
                        + "'myArray': [{'$binary': {'base64': 'S2Fma2Egcm9ja3Mh', 'subType': '00'}}, "
                        + "  {'$date': {'$numberLong': '1577863627000'}}, {'$numberDecimal': '12345.6789'}], "
                        + "'myBytes': {'$binary': {'base64': 'S2Fma2Egcm9ja3Mh', 'subType': '00'}}, "
                        + "'myDate': {'$date': {'$numberLong': '1577863627000'}}, "
                        + "'myDecimal': {'$numberDecimal': '12345.6789'}}";

        String expectedDocString = "{\"myInt\": 1, \"myString\": \"some foo bla text\", \"myDouble\": 20.21, \"mySubDoc\": {\"A\": \"S2Fma2Egcm9ja3Mh\", \"B\": \"2020-01-01T07:27:07Z\", \"C\": \"12345.6789\"}, \"myArray\": [\"S2Fma2Egcm9ja3Mh\", \"2020-01-01T07:27:07Z\", \"12345.6789\"], \"myBytes\": \"S2Fma2Egcm9ja3Mh\", \"myDate\": \"2020-01-01T07:27:07Z\", \"myDecimal\": \"12345.6789\"}";

        Object o = BsonToJsonConverter.bsonToJsonConverter(format(documentString, 1));
        assertEquals(expectedDocString, o.toString());
    }
}
