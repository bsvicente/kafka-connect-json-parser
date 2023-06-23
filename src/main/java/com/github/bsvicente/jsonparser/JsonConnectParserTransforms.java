package com.github.bsvicente.jsonparser;

import com.github.bsvicente.jsonparser.processor.JsonBlacklistFilter;
import com.github.bsvicente.jsonparser.core.JsonToSchemaConverter;
import com.github.bsvicente.jsonparser.core.JsonToStructConverter;
import com.github.bsvicente.jsonparser.processor.JsonSortFilter;
import com.google.gson.JsonElement;
import com.google.gson.JsonParser;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.transforms.Transformation;

import java.io.ByteArrayInputStream;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

@Slf4j
public class JsonConnectParserTransforms<R extends ConnectRecord<R>> implements Transformation<R> {
    JsonConnectParserTransformsConfig config;

    @Override
    public R apply(R r) {

        var type = r.value().getClass().getName();

        log.debug("Converting record from " + type);

        if (r.value() instanceof String) {

            JsonElement jsonElement = JsonParser.parseString((String) r.value());

            return recordBuilder(r, jsonElement);

        }
        if (r.value() instanceof ByteArrayInputStream) {

            JsonElement jsonElement = JsonParser.parseString(Arrays.toString(((ByteArrayInputStream) r.value()).readAllBytes()));

            return recordBuilder(r, jsonElement);

        } else {

            log.debug("Unexpected message type: {}", r.value().getClass().getCanonicalName());

            return r;
        }
    }

    private R recordBuilder(R r, JsonElement jsonElement) {
        var message = mapMessage(jsonElement);

        return r.newRecord(r.topic(), r.kafkaPartition(), r.keySchema(), r.key(),
                (Schema) message.get("schema"), message.get("message"), r.timestamp());
    }

    private HashMap mapMessage(JsonElement jsonElement) {
        log.debug("Received text: {}", jsonElement.toString());

        var result = new HashMap<>();

        log.info("Parsing JSON element: " + jsonElement.toString() + " as JSON element type: " + jsonElement.getClass().getSimpleName());

        JsonBlacklistFilter.builder()
                .blacklist(config.blacklistFilter)
                .build()
                .filterElements(jsonElement);

        JsonElement sortedJsonElement = config.sortFields ? JsonSortFilter.sortJsonElement(jsonElement) : jsonElement;

        var schema = JsonToSchemaConverter.generateSchemaFromJson(sortedJsonElement, config.entityName);
        result.put("schema", schema);

        Struct message = null;
        if (!sortedJsonElement.isJsonNull())
            message = JsonToStructConverter.convertJsonToStruct(sortedJsonElement, schema);

        result.put("message", message);

        return result;
    }


    @Override
    public ConfigDef config() {
        return JsonConnectParserTransformsConfig.config();
    }

    @Override
    public void close() {
    }

    @Override
    public void configure(Map<String, ?> map) {
        this.config = new JsonConnectParserTransformsConfig(map);

    }
}
