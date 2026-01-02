package it.unimi.tpp;

import it.unimi.adminclient.RestClient;
import it.unimi.adminserver.model.Plant;
import it.unimi.adminserver.model.PollutionRecord;
import it.unimi.grpc.PlantCoordinationServiceGrpc;
import it.unimi.grpc.PlantCoordinationServiceOuterClass.*;
import it.unimi.rep.EnergyRequest;
import it.unimi.rep.EnergyRequestMapper;
import it.unimi.simulators.Measurement;
import it.unimi.simulators.PollutionSensor;

import org.eclipse.paho.client.mqttv3.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.grpc.*;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.time.Instant;
import java.util.*;

public class ThermalPowerPlant {

    private static final Logger logger =
            LoggerFactory.getLogger(ThermalPowerPlant.class);

    private final int id;
    private final int portNumber;
    private final ThermalPlantState plantState = new ThermalPlantState();

    private RestClient restClient;
    private PollutionSensor sensor;
    private MqttClient mqttClient;

    private final List<AverageMeasurement> averages = new ArrayList<>();
    private List<Plant> otherPlants;
    private final Map<Integer, PlantCoordinationServiceGrpc.PlantCoordinationServiceBlockingStub> stubs =
            new HashMap<>();

    private volatile boolean stopFlag = false;

    public ThermalPowerPlant(int id) {
        this.id = id;
        this.portNumber = 50051 + id;
    }

    public void start() throws Exception {
        logger.info("Starting ThermalPowerPlant {}", id);

        SlidingWindowBuffer buffer = new SlidingWindowBuffer();
        restClient = new RestClient("localhost", 8080);

        Plant plant = new Plant(id, portNumber);
        otherPlants = restClient.addPlantAndGetNetwork(plant);

        startGRPCServer();
        startGRPCClient();

        sensor = new PollutionSensor(buffer);
        sensor.start();
        logger.info("Pollution sensor started");

        startWindowConsumer(buffer);
        startPublisherThread();
        connectMQTT();
    }

    private void connectMQTT() throws MqttException {
        mqttClient = new MqttClient("tcp://localhost:1883", "TPP-" + id);
        mqttClient.connect();

        mqttClient.subscribe("energy/requests", (topic, message) -> {
            EnergyRequest req =
                    EnergyRequestMapper.deserialize(new String(message.getPayload()));
            logger.debug("Energy request received {}", req);
            handleEnergyRequest(req);
        });

        logger.info("Connected to MQTT broker");
    }

    private void handleEnergyRequest(EnergyRequest request) {
        if (plantState.tryAcquire(request.getTimestamp())) {
            logger.info("Accepted energy request {}", request);
            new Thread(() -> produceEnergy(request)).start();
        } else {
            logger.warn("Plant busy, request ignored {}", request);
        }
    }

    private void produceEnergy(EnergyRequest request) {
        try {
            Thread.sleep(request.getAmount() * 10L);
            logger.info("Energy production completed: {} kWh", request.getAmount());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            logger.error("Energy production interrupted", e);
        } finally {
            plantState.release(request.getTimestamp());
        }
    }

    private void startPublisherThread() {
        new Thread(() -> {
            while (!stopFlag) {
                try {
                    List<AverageMeasurement> snapshot = new ArrayList<>(averages);
                    averages.clear();

                    for (AverageMeasurement avg : snapshot) {
                        restClient.addPollutionRecord(
                                new PollutionRecord(
                                        avg.getPlantId(),
                                        avg.getValue(),
                                        Instant.ofEpochMilli(avg.getTimestamp())
                                )
                        );
                    }

                    Thread.sleep(2000);
                } catch (Exception e) {
                    logger.error("Error publishing pollution data", e);
                }
            }
        }).start();
    }

    private void startWindowConsumer(SlidingWindowBuffer buffer) {
        new Thread(() -> {
            while (!stopFlag) {
                List<Measurement> window = buffer.extractWindowIfAvailable();
                if (window != null) {
                    averages.add(AverageMeasurement.from(window, id));
                }
            }
        }).start();
    }

    private void startGRPCServer() throws Exception {
        Server server = ServerBuilder.forPort(portNumber)
                .addService(new PlantCoordinationServiceImpl())
                .build()
                .start();

        logger.info("gRPC server started on port {}", portNumber);
    }

    private void startGRPCClient() {
        for (Plant p : otherPlants) {
            if (p.getId() == id) continue;

            ManagedChannel channel =
                    ManagedChannelBuilder.forAddress("localhost", p.getPort())
                            .usePlaintext()
                            .build();

            stubs.put(
                    p.getId(),
                    PlantCoordinationServiceGrpc.newBlockingStub(channel)
            );

            logger.info("gRPC client connected to plant {}", p.getId());
        }
    }

    public void stopPlant() {
        logger.info("Stopping ThermalPowerPlant {}", id);
        stopFlag = true;

        if (sensor != null) {
            sensor.stopMeGently();
        }

        try {
            if (mqttClient != null && mqttClient.isConnected()) {
                mqttClient.disconnect();
            }
        } catch (MqttException e) {
            logger.error("Error during MQTT disconnect", e);
        }

        logger.info("ThermalPowerPlant {} stopped", id);
    }

    private static int insertID() {
        try {
            BufferedReader reader =
                    new BufferedReader(new InputStreamReader(System.in));
            int id;

            do {
                System.out.print("Enter plant ID (>0): ");
                id = Integer.parseInt(reader.readLine());
            } while (id <= 0);

            return id;

        } catch (Exception e) {
            throw new RuntimeException("Invalid plant ID", e);
        }
    }

    public static void main(String[] args) {
        try {
            int id = insertID();
            ThermalPowerPlant plant = new ThermalPowerPlant(id);
            plant.start();

            Thread.sleep(120_000);
            plant.stopPlant();

        } catch (Exception e) {
            LoggerFactory.getLogger(ThermalPowerPlant.class)
                    .error("Fatal error in ThermalPowerPlant main", e);
        }
    }
}
