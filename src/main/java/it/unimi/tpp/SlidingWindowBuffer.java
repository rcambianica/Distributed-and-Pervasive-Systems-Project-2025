package it.unimi.tpp;

import it.unimi.simulators.Buffer;
import it.unimi.simulators.Measurement;

import java.util.ArrayList;
import java.util.List;

/**
 * la classe implementa un buffer di tipo sliding window gestito da un monitor,
 * seguendo le specifiche progetto.
 */
public class SlidingWindowBuffer implements Buffer {

    private final List<Measurement> buffer = new ArrayList<>();

    @Override
    public synchronized void addMeasurement(Measurement m) {
        buffer.add(m);
        System.out.println("Nuova misurazione: " + m);
    }

    @Override
    public synchronized List<Measurement> readAllAndClean() {
        List<Measurement> all = new ArrayList<>(buffer);
        buffer.clear();
        return all;
    }

    /**
     * come da specifica progetto
     * Sliding window: size=8, overlap=4.
     */
    public synchronized List<Measurement> extractWindowIfAvailable() {
        if (buffer.size() < 8) return null;

        List<Measurement> window = new ArrayList<>(buffer.subList(0, 8));
        buffer.subList(0, 4).clear();
        return window;
    }
}
