package it.unimi.tpp;

import it.unimi.adminclient.RestClient;
import it.unimi.adminserver.model.Plant;
import it.unimi.adminserver.model.PollutionRecord;
import it.unimi.rep.EnergyRequest;
import it.unimi.simulators.Buffer;
import it.unimi.simulators.PollutionSensor;
import it.unimi.simulators.Measurement;

import org.eclipse.paho.client.mqttv3.*;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

public class ThermalPowerPlant {

    private final String id;
    private final ThermalPlantState plantState;
    private final static String adminServerHost = "localhost";
    private final int adminServerPort;

    private RestClient restClient;
    private PollutionSensor sensor;
    private MqttClient mqttClient;
    private Buffer buffer;
    private final List<AverageMeasurement> averages = new ArrayList<>();

    private volatile boolean stopFlag = false;


    public ThermalPowerPlant(String id, int adminServerPort) {
        this.id = id;
        this.adminServerPort = adminServerPort;
        plantState = new ThermalPlantState();
    }

    public void start(Buffer buffer) throws Exception {
        SlidingWindowBuffer slidingBuffer = new SlidingWindowBuffer();
        this.buffer = slidingBuffer;

        System.out.println("Avvio ThermalPlant " + id);

        // 1. Connessione al server REST
        restClient = new RestClient(adminServerHost, adminServerPort);
        Plant plant = new Plant(id, 1);
        List<Plant> otherPlants = restClient.addPlantAndGetNetwork(plant);
        System.out.println("Plants nel network: " + otherPlants);

        // 2. Avvio sensore
        sensor = new PollutionSensor(buffer);
        sensor.start();
        System.out.println("PollutionSensor avviato.");

        // 3. Thread per inviare periodicamente dati generati dal pollution sensor al server REST per simulare produzione CO2 conseguente alla produzione di energia
        Thread publisherThread = new Thread(() -> {
            while (!stopFlag) {
                try {
                    List<AverageMeasurement> toSend = new ArrayList<>(averages);
                    averages.clear();

                    for (AverageMeasurement avg : toSend) {
                        restClient.addPollutionRecord(
                                new PollutionRecord(avg.getPlantId(), avg.getValue(), Instant.ofEpochMilli(avg.getTimestamp()))
                        );
                    }

                    Thread.sleep(2000);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        });
        publisherThread.start();

        //thread consumatore con sliding window
        Thread windowConsumer = new Thread(() -> {
            while (!stopFlag) {
                List<Measurement> window = slidingBuffer.extractWindowIfAvailable();
                if (window != null) {
                    AverageMeasurement avg = AverageMeasurement.from(window, id);
                    averages.add(avg);
                }
            }
        });
        windowConsumer.start();


        // 4. Connessione MQTT
        String brokerUrl = "tcp://localhost:1883";
        mqttClient = new MqttClient(brokerUrl, "ThermalPlant-" + id);
        MqttConnectOptions options = new MqttConnectOptions();
        options.setAutomaticReconnect(true);
        options.setCleanSession(true);
        mqttClient.connect(options);

        mqttClient.subscribe("energy/requests", (topic, message) -> {   //è un thread concorrente a tutti gli effetti
            String payload = new String(message.getPayload());
            System.out.println("Ricevuta richiesta energia: " + payload);
            handleEnergyRequest(payload);
        });

        System.out.println("ThermalPlant " + id + " avviata correttamente.");
    }

    private void handleEnergyRequest(String payload) {
        try {
            // Parse del formato: EnergyRequest{timestamp=1766942093069, amount=14025kWh}
            int timestampStart = payload.indexOf("timestamp=") + 10;
            int timestampEnd = payload.indexOf(",", timestampStart);
            long timestamp = Long.parseLong(payload.substring(timestampStart, timestampEnd));

            int amountStart = payload.indexOf("amount=") + 7;
            int amountEnd = payload.indexOf("kWh", amountStart);
            int amount = Integer.parseInt(payload.substring(amountStart, amountEnd));

            EnergyRequest request = new EnergyRequest(timestamp, amount);

            // Prova ad acquisire la risorsa
            if (plantState.tryAcquire(request.getTimestamp())) {
                // Avvia thread di produzione energia
                Thread productionThread = new Thread(() -> produceEnergy(request));
                productionThread.start();
            } else {
                System.out.println("Plant " + id + " è BUSY, richiesta ignorata: " + request);
            }
        } catch (Exception e) {
            System.err.println("Formato richiesta non valido: " + payload);
            e.printStackTrace();
        }
    }

    private void produceEnergy(EnergyRequest request) {
        System.out.println("Plant " + id + " inizia produzione per " + request);

        try {
            // Simula tempo di produzione: 1 secondo per kWh
            int productionTimeMs = request.getAmount() * 10;
            Thread.sleep(productionTimeMs);

            System.out.println("Plant " + id + " ha completato produzione di " +
                             request.getAmount() + " kWh (requestId: " + request.getTimestamp() + ")");

        } catch (InterruptedException e) {
            System.err.println("Produzione interrotta per " + request);
            Thread.currentThread().interrupt();
        } finally {
            // Rilascia la risorsa
            plantState.release(request.getTimestamp());
        }
    }

    public void stopPlant() {
        System.out.println("Arresto ThermalPlant " + id);
        stopFlag = true;
        if (sensor != null) sensor.stopMeGently();
        try {
            if (mqttClient != null && mqttClient.isConnected()) mqttClient.disconnect();
        } catch (MqttException e) {
            e.printStackTrace();
        }
        System.out.println("ThermalPlant " + id + " fermata.");
    }

    public static void main(String[] args) {
        try {
            SlidingWindowBuffer buffer = new SlidingWindowBuffer();
            ThermalPowerPlant plant = new ThermalPowerPlant("TP1", 8080);
            plant.start(buffer);
            Thread.sleep(120000);
            plant.stopPlant();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
