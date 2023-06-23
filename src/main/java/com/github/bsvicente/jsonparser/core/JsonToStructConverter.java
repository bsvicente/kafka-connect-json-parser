package com.github.bsvicente.jsonparser.core;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;

import java.util.ArrayList;
import java.util.List;

public class JsonToStructConverter {
    public static Struct convertJsonToStruct(JsonElement json, Schema schema) {
        Struct struct = new Struct(schema);

        for (Field field : schema.fields()) {
            String fieldName = field.name();
            Schema fieldSchema = field.schema();

            if (json.isJsonNull() || !json.getAsJsonObject().has(fieldName)) {
                continue;
            }

            JsonElement fieldJson = json.getAsJsonObject().get(fieldName);
            Object fieldValue = mapJsonElementToField(fieldJson, fieldSchema);
            struct.put(fieldName, fieldValue);
        }

        return struct;
    }

    private static Object mapJsonElementToField(JsonElement json, Schema schema) {
        if (schema.type() == Schema.Type.STRING) {
            return !json.isJsonNull() ? json.getAsString() : null;
        } else if (schema.type() == Schema.Type.INT32) {
            return !json.isJsonNull() ? json.getAsInt() : null;
        } else if (schema.type() == Schema.Type.FLOAT64) {
            return !json.isJsonNull() ? json.getAsDouble() : null;
        } else if (schema.type() == Schema.Type.BOOLEAN) {
            return !json.isJsonNull() ? json.getAsBoolean() : null;
        } else if (schema.type() == Schema.Type.STRUCT) {
            return !json.isJsonNull() ? convertJsonToStruct(json, schema) : null;
        } else if (schema.type() == Schema.Type.ARRAY) {
            JsonArray jsonArray = json.getAsJsonArray();
            List array = new ArrayList<>();
            int i = 0;
            for (JsonElement element : jsonArray) {
                if (schema.valueSchema().type() == Schema.Type.STRUCT) {
                    array.add(convertJsonToStruct(element, schema.valueSchema()));
                } else {
                    array.add(mapJsonElementToField(element, schema.valueSchema()));
                }
            }
            return !json.isJsonNull() ? array : null;
        } else {
            throw new RuntimeException("Unsupported schema type: " + schema.type());
        }
    }

}
