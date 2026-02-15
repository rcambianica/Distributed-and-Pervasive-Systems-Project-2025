package it.unimi.tpp;

import it.unimi.adminserver.model.Pollution;

import java.util.List;

public class PollutionPacket {
    private int plantId;
    private long timestamp;
    private List<Pollution> averages;

    public PollutionPacket(int plantId, long timestamp, List<Pollution> averages) {
        this.plantId = plantId;
        this.timestamp = timestamp;
        this.averages = averages;
    }

    public int getPlantId() { return plantId; }
    public long getTimestamp() { return timestamp; }
    public List<Pollution> getAverages() { return averages; }
}
