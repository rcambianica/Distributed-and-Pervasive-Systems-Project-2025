package it.unimi.adminserver.storage;

import it.unimi.adminserver.model.Pollution;
import it.unimi.adminserver.sync.MyLock;

import java.util.ArrayList;
import java.util.List;

public class PollutionStorage {

    private static final PollutionStorage instance = new PollutionStorage();

    private final List<Pollution> records = new ArrayList<>();
    private final MyLock lock = new MyLock();

    private PollutionStorage() {}

    public static PollutionStorage getInstance() {
        return instance;
    }

    public void addRecord(Pollution rec) {
        try {
            lock.lock();
            try {
                records.add(rec);
            } finally {
                lock.unlock();
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    public List<Pollution> getAll() {
        try {
            lock.lock();
            try {
                return new ArrayList<>(records);
            } finally {
                lock.unlock();
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return List.of();
        }
    }

    public List<Pollution> getByPlant(int plantId) {
        try {
            lock.lock();
            try {
                List<Pollution> result = new ArrayList<>();
                for (Pollution rec : records) {
                    if (rec.getPlantId() == plantId) {
                        result.add(rec);
                    }
                }
                return result;
            } finally {
                lock.unlock();
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return List.of();
        }
    }
}

