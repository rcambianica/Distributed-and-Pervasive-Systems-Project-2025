package it.unimi.adminserver.model;

import java.time.Instant;

public class PollutionRecord {
    private String plantId;
    private double value;
    private Instant timestamp;

    public PollutionRecord() {}

    public PollutionRecord(String plantId, double value, Instant timestamp) {
        this.plantId = plantId;
        this.value = value;
        this.timestamp = timestamp;
    }

    public String getPlantId() { return plantId; }
    public double getValue() { return value; }
    public Instant getTimestamp() { return timestamp; }

    public void setPlantId(String plantId) { this.plantId = plantId; }
    public void setValue(double value) { this.value = value; }
    public void setTimestamp(Instant timestamp) { this.timestamp = timestamp; }
}
