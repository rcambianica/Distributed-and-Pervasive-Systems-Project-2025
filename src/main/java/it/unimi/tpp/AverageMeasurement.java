package it.unimi.tpp;

import it.unimi.simulators.Measurement;

import java.util.List;

public class AverageMeasurement {
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

    private AverageMeasurement(int plantId, double value, long timestamp) {
        this.plantId = plantId;
        this.value = value;
        this.timestamp = timestamp;
    }

    public static AverageMeasurement from(List<Measurement> window, int plantId) {
        double avg = window.stream()
                .mapToDouble(Measurement::getValue)
                .average()
                .orElse(0.0);

        long ts = window.get(window.size() - 1).getTimestamp();
        return new AverageMeasurement(plantId, avg, ts);
    }
}
