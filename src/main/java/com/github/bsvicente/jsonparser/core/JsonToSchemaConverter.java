package com.github.bsvicente.jsonparser.core;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;

import java.util.Map;

@Slf4j
public class JsonToSchemaConverter {

    public static Schema generateSchemaFromJson(JsonElement jsonElement) {
        return generateSchemaFromJson(jsonElement, "");
    }

    @SneakyThrows
    public static Schema generateSchemaFromJson(JsonElement jsonElement, String schemaName) {
        return generateSchemaBuilder(jsonElement, schemaName)
                .optional().build();
    }


    private static SchemaBuilder generateSchemaBuilder(JsonElement jsonElement, String schemaName) {
        SchemaBuilder builder;
        switch (jsonElement.getClass().getSimpleName()) {
            case "JsonPrimitive":
                builder = getSchemaBuilderFromPrimitive(jsonElement.getAsJsonPrimitive());
                break;
            case "JsonNull":
                builder = getSchemaBuilderFromPrimitive(jsonElement.getAsJsonNull());
                break;
            case "JsonArray":
                builder = getSchemaBuilderFromArray(jsonElement.getAsJsonArray(), schemaName);
                break;
            case "JsonObject":
                builder = getSchemaBuilderFromObject(jsonElement.getAsJsonObject(), schemaName);
                break;
            default:
                throw new IllegalArgumentException("Unsupported JSON element type: " + jsonElement.getClass());
        }
        return builder;
    }

    private static SchemaBuilder getSchemaBuilderFromPrimitive(JsonElement element) {
        SchemaBuilder builder;
        if (element.isJsonNull()) {
            builder = SchemaBuilder.struct();
            builder.optional();
        } else {
            switch (element.getAsJsonPrimitive().getClass().getSimpleName()) {
                case "JsonPrimitive":
                    if (element.getAsJsonPrimitive().isBoolean()) {
                        builder = SchemaBuilder.bool();
                    } else if (element.getAsJsonPrimitive().isNumber()) {
                        builder = SchemaBuilder.type(Schema.Type.FLOAT64);
                    } else {
                        builder = SchemaBuilder.string();
                    }
                    break;
                default:
                    builder = SchemaBuilder.string();
                    break;
            }
        }
        return builder;
    }

    private static SchemaBuilder getSchemaBuilderFromArray(JsonArray array, String schemaName) {
        SchemaBuilder builder = SchemaBuilder.type(Schema.Type.ARRAY);
        if (array.size() == 0 || array.get(0).isJsonNull()) {
            builder.optional();
        } else {
            Schema elementBuilder = generateSchemaFromJson(array.get(0), schemaName);
            builder = SchemaBuilder.array(elementBuilder);
        }
        return builder;
    }

    private static SchemaBuilder getSchemaBuilderFromObject(JsonObject object, String schemaName) {

        SchemaBuilder builder = SchemaBuilder.struct();

        for (Map.Entry<String, JsonElement> entry : object.entrySet()) {
            String key = entry.getKey();
            JsonElement value = entry.getValue();
            if (value.isJsonNull()) {
                builder.field(key, Schema.OPTIONAL_STRING_SCHEMA);
            } else {
                builder.field(key, generateSchemaFromJson(value, StringUtils.capitalize(schemaName) + "AllOf" + StringUtils.capitalize(key)));
            }
        }
        return builder.name(schemaName);
    }
}

