package it.unimi.adminserver.model;

import it.unimi.simulators.Measurement;

import java.time.Instant;
import java.util.List;

public class Pollution {
    private final int plantId;
    private final double value;
    private final long timestamp;

    public int getPlantId() {
        return plantId;
    }

    public double getValue() {
        return value;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public Pollution(int plantId, double value, long timestamp) {
        this.plantId = plantId;
        this.value = value;
        this.timestamp = timestamp;
    }

    /**
     * Computes average measurement from a time‑ordered window
     */
    public static Pollution from(List<Measurement> window, int plantId) {
        double avg = window.stream()
                .mapToDouble(Measurement::getValue)
                .average()
                .orElse(0.0);

        long ts = window.get(window.size() - 1).getTimestamp();
        return new Pollution(plantId, avg, ts);
    }
}
