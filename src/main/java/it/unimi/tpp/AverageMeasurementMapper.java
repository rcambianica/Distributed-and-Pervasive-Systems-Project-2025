package it.unimi.tpp;

public class AverageMeasurementMapper {


    public static String serialize(AverageMeasurement avg) {
        return avg.getPlantId() + "," +
                avg.getTimestamp() + "," +
                avg.getValue();
    }

    public static AverageMeasurement deserialize(String payload) {
        String[] parts = payload.split(",");
        int plantId = Integer.parseInt(parts[0]);
        long timestamp = Long.parseLong(parts[1]);
        double value = Double.parseDouble(parts[2]);

        return new AverageMeasurement(plantId, value, timestamp);
    }
}