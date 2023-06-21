package com.github.bsvicente.jsonparser;

import com.github.bsvicente.jsonparser.processor.JsonBlacklistFilter;
import com.github.bsvicente.jsonparser.processor.JsonToSchemaConverter;
import com.github.bsvicente.jsonparser.processor.JsonToStructConverter;
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

    public static final String[] blacklist = {"_links", "_elements"};

    public static final String FIELD_KEY_CONFIG = "key";
    public static final String FIELD_BLACKLIST_CONFIG = "blacklist";
    public static final ConfigDef CONFIG_DEF = new ConfigDef()
            .define(FIELD_KEY_CONFIG, ConfigDef.Type.STRING, null, ConfigDef.Importance.MEDIUM,
                    "Transforms Plain Json to Connect Struct");
//            .define(FIELD_BLACKLIST_CONFIG, ConfigDef.Type.STRING, null, ConfigDef.Importance.MEDIUM,
//                    "Apply blacklist filter on Json before parsing.");

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
                .blacklist(blacklist)
                .build()
                .filterElements(jsonElement);

        var schema = JsonToSchemaConverter.generateSchemaFromJson(jsonElement);
        result.put("schema", schema);

        Struct message = null;
        if (!jsonElement.isJsonNull())
            message = JsonToStructConverter.convertJsonToStruct(jsonElement, schema);

        result.put("message", message);

        return result;
    }


    @Override
    public ConfigDef config() {
        return CONFIG_DEF;
    }

    @Override
    public void close() {
    }

    @Override
    public void configure(Map<String, ?> map) {

    }
}
