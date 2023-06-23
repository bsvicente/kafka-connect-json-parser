package com.github.bsvicente.jsonparser.processor;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import lombok.Builder;
import lombok.NonNull;
import lombok.SneakyThrows;

import java.util.Set;

@Builder
public class JsonBlacklistFilter {

    @NonNull
    private Set<String> blacklist;

    @SneakyThrows
    public JsonElement filterElements(JsonElement jsonElement) {

        if (jsonElement.isJsonObject()) {
            this.blacklist.stream()
                    .filter(jsonElement.getAsJsonObject()::has)
                    .forEach(jsonElement.getAsJsonObject()::remove);

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
