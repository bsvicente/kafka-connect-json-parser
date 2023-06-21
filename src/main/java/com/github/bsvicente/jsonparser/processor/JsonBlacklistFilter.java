package com.github.bsvicente.jsonparser.processor;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import lombok.Builder;

import java.util.Arrays;

@Builder
public class JsonBlacklistFilter {

    private String[] blacklist;

    public JsonElement filterElements(JsonElement jsonElement) {

        if (jsonElement.isJsonObject()) {
            Arrays.stream(this.blacklist).filter(jsonElement.getAsJsonObject()::has).forEach(jsonElement.getAsJsonObject()::remove);

            jsonElement.getAsJsonObject().entrySet().forEach(element -> filterElements(element.getValue()));

        } else if (jsonElement.isJsonArray()) {
            JsonArray jsonArray = jsonElement.getAsJsonArray();
            for (JsonElement arr : jsonArray) {
                filterElements(arr);
            }

        }
        return jsonElement;
    }
}
