package com.github.bsvicente.jsonparser;

import com.github.bsvicente.jsonparser.processor.JsonToSchemaConverter;
import com.github.bsvicente.jsonparser.processor.JsonToStructConverter;
import com.google.gson.JsonElement;
import com.google.gson.JsonParser;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.transforms.Transformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public class JsonConnectParserTransforms<R extends ConnectRecord<R>> implements Transformation<R> {
    public static final String FIELD_KEY_CONFIG = "key";
    public static final ConfigDef CONFIG_DEF = new ConfigDef()
            .define(FIELD_KEY_CONFIG, ConfigDef.Type.STRING, null, ConfigDef.Importance.MEDIUM,
                    "Transforms Plain Json to Connect Struct");

    private static final Logger LOG = LoggerFactory.getLogger(JsonConnectParserTransforms.class);

    @Override
    public R apply(R r) {

        var type = r.value().getClass().getName();

        LOG.debug("Converting record from " + type);

        if (r.value() instanceof String) {

            JsonElement jsonElement = JsonParser.parseString((String) r.value());

            LOG.debug("Received text: {}", jsonElement.toString());

            var schema = JsonToSchemaConverter.generateSchemaFromJson(jsonElement);

            var message = JsonToStructConverter.convertJsonToStruct(jsonElement, schema);

            return r.newRecord(r.topic(), r.kafkaPartition(), null, r.key(),
                    schema, message, r.timestamp());

        } else {

            LOG.debug("Unexpected message type: {}", r.value().getClass().getCanonicalName());

            return r;
        }
    }


    @Override
    public ConfigDef config() {
        return CONFIG_DEF;
    }

    @Override
    public void close() {}

    @Override
    public void configure(Map<String, ?> map) {

    }
}
