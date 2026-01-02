package it.unimi.adminserver.controller;

import it.unimi.adminserver.model.PollutionRecord;
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
    public List<PollutionRecord> getAll() {
        return storage.getAll();
    }

    @GetMapping("/{plantId}")
    public List<PollutionRecord> getByPlant(@PathVariable int plantId) {
        return storage.getByPlant(plantId);
    }

    @PostMapping
    public ResponseEntity<?> addRecord(@RequestBody PollutionRecord record) {
        storage.addRecord(record);
        return ResponseEntity.status(HttpStatus.CREATED).build();
    }

    /**
     * Restituisce la media delle emissioni di CO2 in un intervallo temporale [t1, t2].
     * Corrisponde al metodo che il tuo Administration Client chiama con getAverageCO2.
     **/
    @GetMapping("/average")
    public ResponseEntity<Double> getAverage(
            @RequestParam long t1,
            @RequestParam long t2
    ) {
        List<PollutionRecord> all = storage.getAll();

        double avg = all.stream()
                .filter(r -> {
                    Instant ts = r.getTimestamp();
                    return !ts.isBefore(Instant.ofEpochMilli(t1)) &&
                            !ts.isAfter(Instant.ofEpochMilli(t2));
                })
                .mapToDouble(PollutionRecord::getValue)
                .average()
                .orElse(0.0);

        return ResponseEntity.ok(avg);
    }
}
