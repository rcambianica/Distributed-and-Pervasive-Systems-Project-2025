// storage/PollutionStorage.java
package it.unimi.adminserver.storage;

import it.unimi.adminserver.model.PollutionRecord;
import java.util.*;

public class PollutionStorage {
    private static final PollutionStorage instance = new PollutionStorage();
    private final List<PollutionRecord> records = new ArrayList<>();

    private PollutionStorage() {}

    public static PollutionStorage getInstance() {
        return instance;
    }

    public synchronized void addRecord(PollutionRecord rec) {
        records.add(rec);
    }

    public synchronized List<PollutionRecord> getAll() {
        return new ArrayList<>(records);
    }

    public synchronized List<PollutionRecord> getByPlant(String plantId) {
        List<PollutionRecord> result = new ArrayList<>();
        for (PollutionRecord rec : records) {
            if (rec.getPlantId().equals(plantId)) {
                result.add(rec);
            }
        }
        return result;
    }
}
