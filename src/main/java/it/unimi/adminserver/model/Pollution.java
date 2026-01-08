package it.unimi.adminserver.model;

import java.time.Instant;

public class Pollution {
    private int plantId;
    private double value;
    private Instant timestamp;

    public Pollution() {}

    public Pollution(int plantId, double value, Instant timestamp) {
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
