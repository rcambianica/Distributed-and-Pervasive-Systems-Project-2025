package it.unimi.adminserver.controller;

import it.unimi.adminserver.model.Plant;
import it.unimi.adminserver.storage.PlantStorage;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.List;

@RestController
@RequestMapping("/plants")
public class PlantController {

    private final PlantStorage storage = PlantStorage.getInstance();

    @GetMapping
    public ResponseEntity<List<Plant>> getAllPlants() throws InterruptedException {
        return ResponseEntity.ok(storage.getAllPlants());
    }

    @PostMapping
    public ResponseEntity<?> addPlant(@RequestBody Plant plant) throws InterruptedException {
        try {
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

    @PostMapping("/{id}/heartbeat")
    public ResponseEntity<Void> heartbeat(@PathVariable int id) throws InterruptedException {
        Plant p = PlantStorage.getInstance().get(id);
        if (p != null) {
            p.heartbeat();
            return ResponseEntity.ok().build();
        }
        return ResponseEntity.notFound().build();
    }
}
