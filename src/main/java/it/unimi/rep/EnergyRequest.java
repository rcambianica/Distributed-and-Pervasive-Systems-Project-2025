package it.unimi.rep;

import java.time.Instant;

public class EnergyRequest {
    private int amount; // kWh
    private long timestamp; // ms

    public EnergyRequest(int amount) {
        this.amount = amount;
        this.timestamp = Instant.now().toEpochMilli();
    }

    public int getAmount() {
        return amount;
    }

    public long getTimestamp() {
        return timestamp;
    }

    @Override
    public String toString() {
        return "EnergyRequest{" +
                "amount=" + amount +
                ", timestamp=" + timestamp +
                '}';
    }
}
