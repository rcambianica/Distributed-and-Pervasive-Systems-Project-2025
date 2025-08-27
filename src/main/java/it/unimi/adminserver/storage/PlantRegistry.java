package it.unimi.adminserver.storage;

import it.unimi.adminserver.model.Plant;
import java.util.*;

public class PlantRegistry {
    private static final PlantRegistry instance = new PlantRegistry();
    private final List<Plant> plants = new ArrayList<>();

    private PlantRegistry() {}

    public static PlantRegistry getInstance() {
        return instance;
    }

    public synchronized List<Plant> getPlants() {
        return new ArrayList<>(plants);
    }

    public synchronized boolean addPlant(Plant p) {
        for (Plant existing : plants) {
            if (existing.getId().equals(p.getId())) {
                return false; // duplicate
            }
        }
        plants.add(p);
        return true;
    }

    public synchronized boolean removePlant(String id) {
        return plants.removeIf(p -> p.getId().equals(id));
    }
}
