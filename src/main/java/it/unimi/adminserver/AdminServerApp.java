package it.unimi.adminserver;

import it.unimi.adminserver.model.Plant;
import it.unimi.adminserver.model.Pollution;
import it.unimi.adminserver.storage.PlantStorage;
import it.unimi.adminserver.storage.PollutionStorage;

import it.unimi.tpp.PollutionMapper;
import org.eclipse.paho.client.mqttv3.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import java.util.ArrayList;
import java.util.List;

@SpringBootApplication
public class AdminServerApp {

    private static final Logger logger =
            LoggerFactory.getLogger(AdminServerApp.class);

    private MqttClient mqttClient;
    private static final String MQTT_BROKER = "tcp://localhost:1883";
    private static final String POLLUTION_TOPIC = "pollution/averages";
    private static final long HEARTBEAT_TIMEOUT = 5_000;

    /**
     * Starts MQTT subscriber to persist pollution records
     */
    @PostConstruct
    public void startMQTTSubscriber() {
        try {
            mqttClient = new MqttClient(MQTT_BROKER, "ADMIN-SERVER");
            mqttClient.connect();

            logger.info("Admin Server connected to MQTT broker");

            mqttClient.subscribe(POLLUTION_TOPIC, (topic, message) -> {

                Pollution record = PollutionMapper.deserialize(new String(message.getPayload()));

                PollutionStorage.getInstance().addRecord(record);

                logger.debug(
                        "[MQTT-POLLUTION] plant={} value={} ts={}",
                        record.getPlantId(),
                        record.getValue(),
                        record.getTimestamp()
                );
            });

            logger.info("Admin Server subscribed to topic {}", POLLUTION_TOPIC);

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

                    Thread.sleep(500);

                } catch (Exception e) {
                    logger.error("Failure detector error", e);
                }
            }
        }).start();
    }

    @PreDestroy
    public void shutdown() {
        try {
            if (mqttClient != null && mqttClient.isConnected()) {
                mqttClient.disconnect();
                mqttClient.close();
            }
        } catch (Exception e) {
            logger.warn("Error closing MQTT client", e);
        }
    }


    public static void main(String[] args) {
        SpringApplication.run(AdminServerApp.class, args);
    }
}
