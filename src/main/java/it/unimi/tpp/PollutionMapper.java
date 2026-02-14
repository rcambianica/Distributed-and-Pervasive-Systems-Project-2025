package it.unimi.tpp;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import it.unimi.adminserver.model.Pollution;


public class PollutionMapper {

    private static final Gson gson = new GsonBuilder().create();

    public static String serialize(Pollution measurement) {
        return gson.toJson(measurement);
    }

    public static Pollution deserialize(String json)   {
        return gson.fromJson(json, Pollution.class);
    }
}