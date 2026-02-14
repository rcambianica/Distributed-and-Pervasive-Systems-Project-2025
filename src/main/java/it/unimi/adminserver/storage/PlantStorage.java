package it.unimi.adminserver.storage;

import it.unimi.adminserver.model.Plant;
import it.unimi.adminserver.sync.MyLock;

import java.util.ArrayList;
import java.util.List;

public class PlantStorage {

    private static final PlantStorage instance = new PlantStorage();

    private final List<Plant> plants = new ArrayList<>();
    private final MyLock lock = new MyLock();

    private PlantStorage() {}

    public static PlantStorage getInstance() {
        return instance;
    }

    public List<Plant> getAllPlants() throws InterruptedException {
        lock.lock();
        try {
            return new ArrayList<>(plants);
        } finally {
            lock.unlock();
        }
    }

    /**
     * Returns plant matching id, else null, under lock
     */
    public Plant get(int id) throws InterruptedException {
        lock.lock();
        try {
            for (Plant p : plants) {
                if (p.getId() == id) return p;
            }
            return null;
        } finally {
            lock.unlock();
        }
    }

        /**
         * Adds plant if unique; returns success status
         */
    public boolean addPlant(Plant plant) throws InterruptedException {
        lock.lock();
        try {
            for (Plant p : plants) {
                if (p.getId() == plant.getId()) {
                    return false;
                }
            }
            plants.add(plant);
            return true;
        } finally {
            lock.unlock();
        }
    }

    public boolean removePlant(int id) throws InterruptedException {
        lock.lock();
        try {
            return plants.removeIf(p -> p.getId() == id);
        } finally {
            lock.unlock();
        }
    }
}
