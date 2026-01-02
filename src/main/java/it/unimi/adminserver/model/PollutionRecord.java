package it.unimi.adminserver.model;

import java.time.Instant;

public class PollutionRecord {
    private int plantId;
    private double value;
    private Instant timestamp;

    public PollutionRecord() {}

    public PollutionRecord(int plantId, double value, Instant timestamp) {
        this.plantId = plantId;
        this.value = value;
        this.timestamp = timestamp;
    }

    public int getPlantId() {
        return plantId;
    }

    public double getValue() { return value; }
    public Instant getTimestamp() { return timestamp; }

}
