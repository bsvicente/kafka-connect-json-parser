package com.github.bsvicente.jsonparser.processor;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;

public class JsonSortFilter {

    public static JsonElement sortJsonElement(JsonElement element) {

        if (element.isJsonObject()) {

            JsonObject jsonObject = element.getAsJsonObject();

            List<Map.Entry<String, JsonElement>> entryList = new ArrayList<>(jsonObject.entrySet());
            entryList.sort(Comparator.comparing(Map.Entry::getKey));

            JsonObject sortedObject = new JsonObject();
            for (Map.Entry<String, JsonElement> entry : entryList) {
                sortedObject.add(entry.getKey(), sortJsonElement(entry.getValue()));
            }

            return sortedObject;

        } else if (element.isJsonArray()) {
            JsonArray arrays = element.getAsJsonArray();
            JsonArray sortedArray = new JsonArray();
            arrays.forEach(jsonElement -> sortedArray.add(sortJsonElement(jsonElement)));

            return sortedArray;
        }

        return element;
    }
}
