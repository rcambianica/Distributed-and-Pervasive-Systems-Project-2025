package it.unimi.adminserver.controller;

import it.unimi.adminserver.model.Plant;
import it.unimi.adminserver.storage.PlantStorage;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.List;

@RestController
@RequestMapping("/plants")
public class PlantController {

    private final PlantStorage storage = new PlantStorage();

    @GetMapping
    public ResponseEntity<List<Plant>> getAllPlants() throws InterruptedException {
        return ResponseEntity.ok(storage.getAllPlants());
    }

    @PostMapping
    public ResponseEntity<?> addPlant(@RequestBody Plant plant) throws InterruptedException {
        try {
            // Validate incoming objects using model setters if not already invoked by constructor
            // Some serializers may bypass constructor, so enforce here
            plant.setId(plant.getId());
            plant.setPort(plant.getPort());
        } catch (IllegalArgumentException e) {
            return ResponseEntity.badRequest().body(e.getMessage());
        }
        boolean added = storage.addPlant(plant);
        if (!added) {
            return ResponseEntity.status(409).body("Plant with same id already exists");
        }
        return ResponseEntity.ok(storage.getAllPlants());
    }

    @DeleteMapping("/{id}")
    public ResponseEntity<?> removePlant(@PathVariable int id) throws InterruptedException {
        boolean removed = storage.removePlant(id);
        if (!removed) {
            return ResponseEntity.status(404).body("Plant not found");
        }
        return ResponseEntity.ok("Removed plant " + id);
    }
}
