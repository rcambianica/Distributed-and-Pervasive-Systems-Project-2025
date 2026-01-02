package it.unimi.rep;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.checkerframework.checker.nullness.qual.NonNull;

public class EnergyRequestMapper {
    private static final Gson gson = new GsonBuilder().create();

    public static @NonNull String serialize(EnergyRequest request) {
        return gson.toJson(request);
    }

    public static EnergyRequest deserialize(String json) {
        return gson.fromJson(json, EnergyRequest.class);
    }
}
