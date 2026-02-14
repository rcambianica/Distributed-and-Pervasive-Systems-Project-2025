package it.unimi.adminserver.controller;

import it.unimi.adminserver.model.Pollution;
import it.unimi.adminserver.storage.PollutionStorage;
import org.springframework.http.*;
import org.springframework.web.bind.annotation.*;

import java.time.Instant;
import java.util.List;

@RestController
@RequestMapping("/pollution")
public class PollutionController {

    private final PollutionStorage storage = PollutionStorage.getInstance();

    @GetMapping
    public List<Pollution> getAll() {
        return storage.getAll();
    }

    @GetMapping("/{plantId}")
    public List<Pollution> getByPlant(@PathVariable int plantId) {
        return storage.getByPlant(plantId);
    }

    @GetMapping("/average")
    public ResponseEntity<Double> getAverage(
            @RequestParam long t1,
            @RequestParam long t2
    ) {
        List<Pollution> all = storage.getAll();

        // Filters pollution data by timestamp range
        double avg = all.stream()
                .filter(r -> {
                    Instant ts = Instant.ofEpochMilli(r.getTimestamp());
                    return !ts.isBefore(Instant.ofEpochMilli(t1)) &&
                            !ts.isAfter(Instant.ofEpochMilli(t2));
                })
                .mapToDouble(Pollution::getValue)
                .average()
                .orElse(0.0);

        return ResponseEntity.ok(avg);
    }
}
