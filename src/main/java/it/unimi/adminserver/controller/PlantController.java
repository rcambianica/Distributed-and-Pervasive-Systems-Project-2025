// controller/PlantController.java
package it.unimi.adminserver.controller;

import it.unimi.adminserver.model.Plant;
import it.unimi.adminserver.storage.PlantRegistry;
import org.springframework.http.*;
import org.springframework.web.bind.annotation.*;

import java.util.List;

@RestController
@RequestMapping("/plants")
public class PlantController {

    private final PlantRegistry registry = PlantRegistry.getInstance();

    @GetMapping
    public List<Plant> getPlants() {
        return registry.getPlants();
    }

    @PostMapping
    public ResponseEntity<?> addPlant(@RequestBody Plant plant) {
        boolean added = registry.addPlant(plant);
        if (!added) {
            return ResponseEntity.status(HttpStatus.CONFLICT)
                    .body("Plant with same ID already exists");
        }
        return ResponseEntity.ok(registry.getPlants());
    }

    @DeleteMapping("/{id}")
    public ResponseEntity<?> removePlant(@PathVariable String id) {
        boolean removed = registry.removePlant(id);
        if (!removed) {
            return ResponseEntity.status(HttpStatus.NOT_FOUND)
                    .body("Plant not found");
        }
        return ResponseEntity.noContent().build();
    }
}
