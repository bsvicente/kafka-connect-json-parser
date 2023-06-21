package com.github.bsvicente.jsonparser;

import com.github.bsvicente.jsonparser.processor.JsonBlacklistFilter;
import com.github.bsvicente.jsonparser.processor.JsonToSchemaConverter;
import com.github.bsvicente.jsonparser.processor.JsonToStructConverter;
import com.google.gson.JsonElement;
import com.google.gson.JsonParser;
import lombok.extern.java.Log;
import org.apache.kafka.connect.data.Struct;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;

@Log
class JsonConnectParserTransformsTest {


    @Test
    public void validateContent() throws IOException {

        final String[] blacklist = {"_links", "_elements"};

        var file = new File("src/test/resources/cloudevents.json");

        JsonElement jsonElement = JsonParser.parseString(Files.readString(file.toPath()));

        JsonBlacklistFilter.builder()
                .blacklist(blacklist)
                .build()
                .filterElements(jsonElement);

        log.info("Parsing JSON element: " + jsonElement.toString() + " as JSON element type: " + jsonElement.getClass().getSimpleName());

        var schema = JsonToSchemaConverter.generateSchemaFromJson(jsonElement);

        var message = JsonToStructConverter.convertJsonToStruct(jsonElement, schema);

        log.info(message.toString());
    }

    @Test
    public void validateNullContent() throws IOException {

        var file = new File("src/test/resources/NullCE.json");

        JsonElement jsonElement = JsonParser.parseString(Files.readString(file.toPath()));

        log.info("Parsing JSON element: " + jsonElement.toString() + " as JSON element type: " + jsonElement.getClass().getSimpleName());

        var schema = JsonToSchemaConverter.generateSchemaFromJson(jsonElement);

        Struct message =  new Struct(schema); //JsonToStructConverter.convertJsonToStruct(jsonElement, schema);

        log.info(message.toString());
    }

}