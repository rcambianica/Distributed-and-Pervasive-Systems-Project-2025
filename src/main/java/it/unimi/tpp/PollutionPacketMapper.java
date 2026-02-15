package it.unimi.tpp;

import com.google.gson.Gson;

public class PollutionPacketMapper {
    private static final Gson gson = new Gson();

    public static String serialize(PollutionPacket p) {
        return gson.toJson(p);
    }

    public static PollutionPacket deserialize(String json) {
        return gson.fromJson(json, PollutionPacket.class);
    }
}
