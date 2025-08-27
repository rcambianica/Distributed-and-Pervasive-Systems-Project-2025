// controller/PollutionController.java
package it.unimi.adminserver.controller;

import it.unimi.adminserver.model.PollutionRecord;
import it.unimi.adminserver.storage.PollutionStorage;
import org.springframework.http.*;
import org.springframework.web.bind.annotation.*;

import java.util.List;

@RestController
@RequestMapping("/pollution")
public class PollutionController {

    private final PollutionStorage storage = PollutionStorage.getInstance();

    @GetMapping
    public List<PollutionRecord> getAll() {
        return storage.getAll();
    }

    @GetMapping("/{plantId}")
    public List<PollutionRecord> getByPlant(@PathVariable String plantId) {
        return storage.getByPlant(plantId);
    }

    @PostMapping
    public ResponseEntity<?> addRecord(@RequestBody PollutionRecord record) {
        storage.addRecord(record);
        return ResponseEntity.status(HttpStatus.CREATED).build();
    }
}
