package it.unimi.rep;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;


public class EnergyRequestMapper {
    private static final Gson gson = new GsonBuilder().create();

    public static String serialize(EnergyRequest request) {
        return gson.toJson(request);
    }

    public static EnergyRequest deserialize(String json) {
        return gson.fromJson(json, EnergyRequest.class);
    }
}
