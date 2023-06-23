package com.github.bsvicente.jsonparser;

import com.github.bsvicente.jsonparser.processor.JsonBlacklistFilter;
import com.github.bsvicente.jsonparser.core.JsonToSchemaConverter;
import com.github.bsvicente.jsonparser.core.JsonToStructConverter;
import com.github.bsvicente.jsonparser.processor.JsonSortFilter;
import com.google.gson.JsonElement;
import com.google.gson.JsonParser;
import lombok.extern.java.Log;
import org.apache.kafka.connect.data.Struct;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

@Log
class JsonConnectParserTransformsTest {


    @Test
    public void validateContent() throws IOException {

        final Set<String> blacklist = new HashSet<>(Arrays.asList("_links", "_elements"));

        var file = new File("src/test/resources/cloudevents.json");

        JsonElement jsonElement = JsonParser.parseString(Files.readString(file.toPath()));

        log.info("Parsing JSON element: " + jsonElement.toString() + " as JSON element type: " + jsonElement.getClass().getSimpleName());

        JsonBlacklistFilter.builder()
                .blacklist(blacklist)
                .build()
                .filterElements(jsonElement);

        JsonElement sortedJsonElement = JsonSortFilter.sortJsonElement(jsonElement);

        var schema = JsonToSchemaConverter.generateSchemaFromJson(sortedJsonElement, null);

        var message = JsonToStructConverter.convertJsonToStruct(sortedJsonElement, schema);

        log.info(message.toString());
    }

    @Test
    public void validateNullContent() throws IOException {

        var file = new File("src/test/resources/NullCE.json");

        JsonElement jsonElement = JsonParser.parseString(Files.readString(file.toPath()));

        log.info("Parsing JSON element: " + jsonElement.toString() + " as JSON element type: " + jsonElement.getClass().getSimpleName());

        JsonElement sortedJsonElement = JsonSortFilter.sortJsonElement(jsonElement);

        var schema = JsonToSchemaConverter.generateSchemaFromJson(sortedJsonElement);

        Struct message =  new Struct(schema); //JsonToStructConverter.convertJsonToStruct(jsonElement, schema);

        log.info(message.toString());
    }

}