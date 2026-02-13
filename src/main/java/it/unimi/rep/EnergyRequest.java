package it.unimi.rep;

import java.time.Instant;

public class EnergyRequest {
    private final long timestamp;   //concettualmente svolge anche ruolo di id della richiesta
    private final int amount;

    public EnergyRequest(long timestamp, int amount) {
        this.timestamp = timestamp;
        this.amount = amount;
    }

    public EnergyRequest(int amount) {
        this.timestamp = Instant.now().toEpochMilli();
        this.amount = amount;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public int getAmount() {
        return amount;
    }

    @Override
    public String toString() {
        return "EnergyRequest{" +
                "timestamp=" + timestamp +
                ", amount=" + amount;
    }
}
