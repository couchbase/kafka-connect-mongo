/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mongodb;

import com.mongodb.BasicDBObject;
import org.bson.BsonDocument;
import org.bson.BsonDouble;
import org.bson.BsonInt32;
import org.bson.BsonInt64;
import org.bson.BsonObjectId;
import org.bson.BsonString;
import org.bson.BsonSymbol;
import org.bson.BsonValue;
import org.bson.json.JsonMode;
import org.bson.json.JsonWriterSettings;

import java.util.function.Function;

/**
 * A class responsible for serialization of message keys and values to MongoDB compatible JSON
 *
 * @author Jiri Pechanec
 */
class JsonSerialization {

    @FunctionalInterface
    public interface Transformer extends Function<BsonDocument, String> {
    }

    private static final String ID_FIELD_NAME = "_id";

    /**
     * Common settings for writing JSON strings using a compact JSON format
     */
    public static final JsonWriterSettings COMPACT_JSON_SETTINGS = JsonWriterSettings.builder()
            .outputMode(JsonMode.STRICT)
            .indent(true)
            .indentCharacters("")
            .newLineCharacters("")
            .build();

    /**
     * Common settings for writing JSON strings using a compact JSON format
     */
    private static final JsonWriterSettings SIMPLE_JSON_SETTINGS = JsonWriterSettings.builder()
            .outputMode(JsonMode.RELAXED)
            .indent(true)
            .indentCharacters("")
            .newLineCharacters("")
            .build();

    private final Transformer transformer;

    JsonSerialization() {
        transformer = (doc) -> doc.toJson(COMPACT_JSON_SETTINGS);
    }

    public String getDocumentIdSnapshot2(BsonDocument document) {
        if (document == null) {
            return null;
        }

        BsonValue key = document.get(ID_FIELD_NAME);

        Object keyValue;
        switch (key.getBsonType()) {
            case OBJECT_ID: {
                keyValue = stringWrap(((BsonObjectId) key).getValue().toString());
                break;
            }
            case INT32: {
                keyValue = ((BsonInt32) key).getValue();
                break;
            }
            case INT64: {
                keyValue = ((BsonInt64) key).getValue();
                break;
            }
            case DOUBLE: {
                keyValue = ((BsonDouble) key).getValue();
                break;
            }
            case SYMBOL: {
                keyValue = stringWrap(((BsonSymbol) key).getSymbol());
                break;
            }
            case STRING: {
                keyValue = stringWrap(((BsonString) key).getValue());
                break;
            }
            default: {
                return getDocumentIdSnapshot(document);
            }
        }

        System.out.println(keyValue);
        String val = "{\"" + ID_FIELD_NAME + "\"" + ":" + keyValue + "}";
        System.out.println("DBG: createdVal" + val);
        return val;
    }

    public String getDocumentIdChangeStream2(BsonDocument document) {
        if (document == null) {
            return null;
        }

        BsonValue key = document.get(ID_FIELD_NAME);

        Object keyValue;
        switch (key.getBsonType()) {
            case OBJECT_ID: {
                keyValue = stringWrap(((BsonObjectId) key).getValue().toString());
                break;
            }
            case INT32: {
                keyValue = ((BsonInt32) key).getValue();
                break;
            }
            case INT64: {
                keyValue = ((BsonInt64) key).getValue();
                break;
            }
            case DOUBLE: {
                keyValue = ((BsonDouble) key).getValue();
                break;
            }
            case SYMBOL: {
                keyValue = stringWrap(((BsonSymbol) key).getSymbol());
                break;
            }
            case STRING: {
                keyValue = stringWrap(((BsonString) key).getValue());
                break;
            }
            default: {
                return getDocumentIdChangeStream(document);
            }
        }

        System.out.println(keyValue);
        String val = "{\"" + ID_FIELD_NAME + "\"" + ":" + keyValue + "}";
        System.out.println("DBG: createdStreamVal" + val);
        return val;
    }

    private String stringWrap(String val){
        return "\"" + val + "\"";
    }

    public String getDocumentIdSnapshot(BsonDocument document) {
        if (document == null) {
            return null;
        }
        // The serialized value is in format {"_": xxx} so we need to remove the starting dummy field name and closing brace
        final String keyValue = new BasicDBObject("_", document.get(ID_FIELD_NAME)).toJson(SIMPLE_JSON_SETTINGS);
        final int start = 6;
        final int end = keyValue.length() - 1;
        if (!(end > start)) {
            throw new IllegalStateException("Serialized JSON object '" + keyValue + "' is not in expected format");
        }
        return keyValue.substring(start, end);
    }

    public String getDocumentIdChangeStream(BsonDocument document) {
        if (document == null) {
            return null;
        }
        // The serialized value is in format {"_": xxx} so we need to remove the starting dummy field name and closing brace
        final String keyValue = document.toJson(SIMPLE_JSON_SETTINGS);
        final int start = 8;
        final int end = keyValue.length() - 1;
        if (!(end > start)) {
            throw new IllegalStateException("Serialized JSON object '" + keyValue + "' is not in expected format");
        }
        return keyValue.substring(start, end);
    }

    public String getDocumentValue(BsonDocument document) {
        return transformer.apply(document);
    }

    public Transformer getTransformer() {
        return transformer;
    }
}
