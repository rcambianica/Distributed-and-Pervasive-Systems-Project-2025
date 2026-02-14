package it.unimi.tpp;

import it.unimi.simulators.Buffer;
import it.unimi.simulators.Measurement;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

public class SlidingWindowBuffer implements Buffer {

    private static final Logger logger =
            LoggerFactory.getLogger(SlidingWindowBuffer.class);

    private final List<Measurement> buffer = new ArrayList<>();

    @Override
    public synchronized void addMeasurement(Measurement m) {
        buffer.add(m);
        logger.debug("Measurement added: {}", m);
    }

    @Override
    public synchronized List<Measurement> readAllAndClean() {
        List<Measurement> all = new ArrayList<>(buffer);
        buffer.clear();
        return all;
    }

    /**
     * Extracts a sliding window if the buffer exceeds the threshold
     */
    public synchronized List<Measurement> extractWindowIfAvailable() {
        if (buffer.size() < 8) {
            return null;
        }

        List<Measurement> window = new ArrayList<>(buffer.subList(0, 8));
        buffer.subList(0, 4).clear();
        return window;
    }

}
