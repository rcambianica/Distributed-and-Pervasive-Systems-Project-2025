package it.unimi.adminserver;

import it.unimi.adminserver.model.Plant;
import it.unimi.adminserver.model.Pollution;
import it.unimi.adminserver.storage.PlantStorage;
import it.unimi.adminserver.storage.PollutionStorage;

import org.eclipse.paho.client.mqttv3.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import javax.annotation.PostConstruct;
import java.util.ArrayList;
import java.util.List;

@SpringBootApplication
public class AdminServerApp {

    private static final Logger logger =
            LoggerFactory.getLogger(AdminServerApp.class);

    private static final String MQTT_BROKER = "tcp://localhost:1883";
    private static final String POLLUTION_TOPIC = "pollution/averages";
    private static final long HEARTBEAT_TIMEOUT = 15_000;

    @PostConstruct
    public void startMQTTSubscriber() {
        try {
            MqttClient client =
                    new MqttClient(MQTT_BROKER, "ADMIN-SERVER");

            client.connect();
            logger.info("Admin Server connected to MQTT broker");

            client.subscribe(POLLUTION_TOPIC, (topic, message) -> {
                Pollution record =
                        deserializePollution(new String(message.getPayload()));

                PollutionStorage.getInstance().addRecord(record);

                logger.debug(
                        "[MQTT-POLLUTION] plant={} value={} ts={}",
                        record.getPlantId(),
                        record.getValue(),
                        record.getTimestamp()
                );
            });

            logger.info(
                    "Admin Server subscribed to topic {}",
                    POLLUTION_TOPIC
            );

        } catch (Exception e) {
            logger.error("Failed to start MQTT subscriber", e);
        }
    }

    @PostConstruct
    public void startFailureDetector() {
        new Thread(() -> {
            while (true) {
                try {
                    long now = System.currentTimeMillis();

                    List<Integer> toRemove = new ArrayList<>();

                    for (Plant p : PlantStorage.getInstance().getAllPlants()) {
                        if (now - p.getLastHeartbeat() > HEARTBEAT_TIMEOUT) {
                            toRemove.add(p.getId());
                        }
                    }

                    for (int id : toRemove) {
                        logger.warn("Plant {} considered crashed (heartbeat timeout)", id);
                        PlantStorage.getInstance().removePlant(id);
                    }


                    Thread.sleep(5000);

                } catch (Exception e) {
                    logger.error("Failure detector error", e);
                }
            }
        }).start();
    }

    private Pollution deserializePollution(String payload) {
        String[] parts = payload.split(",");

        int plantId = Integer.parseInt(parts[0]);
        long ts = Long.parseLong(parts[1]);
        double value = Double.parseDouble(parts[2]);

        return new Pollution(
                plantId,
                value,
                java.time.Instant.ofEpochMilli(ts)
        );
    }


    public static void main(String[] args) {
        SpringApplication.run(AdminServerApp.class, args);
    }
}
