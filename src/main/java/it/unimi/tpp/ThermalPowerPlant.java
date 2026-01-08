package it.unimi.tpp;

import it.unimi.adminclient.RestClient;
import it.unimi.adminserver.model.Plant;
import it.unimi.adminserver.model.Pollution;
import it.unimi.grpc.PlantCoordinationServiceGrpc;
import it.unimi.grpc.PlantCoordinationServiceGrpc.*;
import it.unimi.grpc.PlantCoordinationServiceOuterClass.*;
import it.unimi.rep.EnergyRequest;
import it.unimi.rep.EnergyRequestMapper;
import it.unimi.simulators.Measurement;
import it.unimi.simulators.PollutionSensor;
import it.unimi.adminserver.sync.MyLock;

import org.eclipse.paho.client.mqttv3.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.grpc.*;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.*;

public class ThermalPowerPlant {

    private static final Logger logger =
            LoggerFactory.getLogger(ThermalPowerPlant.class);

    private final int id;
    private final int portNumber;
    private final ThermalPlantState plantState = new ThermalPlantState();
    private SlidingWindowBuffer buffer;

    private final Map<Long,ElectionContext> electionContexts = new HashMap<>();
    private final MyLock electionLock = new MyLock();
    private final Map<Long,EnergyRequest> pendingRequests = new HashMap<>();

    public static final String REST_SERVER_ADDRESS = "localhost";
    public static final int REST_SERVER_PORT = 8080;
    private static final String ER_TOPIC = "energy/requests";
    private static final String POLLUTION_TOPIC = "pollution/averages";

    private PollutionSensor sensor;
    private final List<AverageMeasurement> averages = new ArrayList<>();

    private RestClient restClient;

    private MqttClient mqttClient;
    private final LinkedList<Runnable> mqttQueue = new LinkedList<>();


    private final MyLock networkLock = new MyLock();
    private static final Map<Integer, Plant> knownPlants = new HashMap<>();
    private final Map<Integer, PlantCoordinationServiceBlockingStub> stubs =
            new HashMap<>();

    private volatile boolean stopFlag = false;

    public ThermalPowerPlant(int id) {
        this.id = id;
        this.portNumber = 5000 + id;
    }

    public int getId() {
        return id;
    }


    /**
     * Determines ID of the next plant to recreate a ring topology used by the Chang&Roberts algorithm.
     */
    private int retrieveSuccessorId() {
        try {
            networkLock.lock();
            try {
                List<Integer> ids = new ArrayList<>(knownPlants.keySet());
                ids.add(this.id);
                Collections.sort(ids);
                int index = ids.indexOf(this.id);
                return ids.get((index + 1) % ids.size());
            } finally {
                networkLock.unlock();
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return this.id;
        }
    }

    private float generateBid() {
        return 0.1f + (float) Math.random() * (0.9f - 0.1f);
    }

    private static int insertID() {
        try {
            RestClient restClient = new RestClient(REST_SERVER_ADDRESS, REST_SERVER_PORT);
            BufferedReader reader =
                    new BufferedReader(new InputStreamReader(System.in));
            int id;

            while (true) {
                System.out.print("Enter plant ID (>0): ");
                id = Integer.parseInt(reader.readLine());

                if (id <= 0) {
                    System.out.println("ID must be > 0");
                    continue;
                }

                List<Plant> network = restClient.getNetwork();

                boolean alreadyUsed = false;
                for (Plant p : network) {
                    if (p.getId() == id) {
                        alreadyUsed = true;
                        break;
                    }
                }

                if (alreadyUsed) {
                    System.out.println("ID already in use by an active plant");
                    continue;
                }

                return id;
            }

        } catch (Exception e) {
            throw new RuntimeException("Invalid plant ID", e);
        }
    }

    /**
     * Updates known plants; logs active sets; guards against interruption
     * **/
    private void initializeNetwork(List<Plant> network) {
        try {
            networkLock.lock();
            try {
                knownPlants.clear();
                for (Plant p : network) {
                    if (p.getId() != this.id) {
                        knownPlants.put(p.getId(), p);
                    }
                }
                logger.info("Initial network view: {}", knownPlants.keySet());
            } finally {
                networkLock.unlock();
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            logger.error("Interrupted during network initialization", e);
        }
    }

    /**
     * Removes gRPC stubs for inactive plants from the cache
     * **/
    private void removeObsoleteStubs(Set<Integer> activeIds) {
        try {
            networkLock.lock();
            try {
                Iterator<Map.Entry<Integer, PlantCoordinationServiceBlockingStub>> it =
                        stubs.entrySet().iterator();
                while (it.hasNext()) {
                    Map.Entry<Integer, PlantCoordinationServiceBlockingStub> entry = it.next();
                    if (!activeIds.contains(entry.getKey())) {
                        it.remove();
                        logger.warn("Removed gRPC stub for plant {}", entry.getKey());
                    }
                }
            } finally {
                networkLock.unlock();
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    /**
     * Updates network view; removes obsolete stubs; restarts gRPC client
     */
    private void updateNetwork(List<Plant> snapshot) {
        Map<Integer, Plant> newView = new HashMap<>();
        for (Plant p : snapshot) {
            if (p.getId() != this.id) {
                newView.put(p.getId(), p);
            }
        }

        try {
            networkLock.lock();
            try {
                for (Map.Entry<Integer, Plant> entry : newView.entrySet()) {
                    Integer pid = entry.getKey();
                    if (!knownPlants.containsKey(pid)) {
                        knownPlants.put(pid, entry.getValue());
                        logger.info("Plant {} joined the network", pid);
                    }
                }

                Iterator<Map.Entry<Integer, Plant>> it =
                        knownPlants.entrySet().iterator();
                while (it.hasNext()) {
                    Map.Entry<Integer, Plant> entry = it.next();
                    if (!newView.containsKey(entry.getKey())) {
                        logger.warn("Plant {} removed from network", entry.getKey());
                        it.remove();
                    }
                }
            } finally {
                networkLock.unlock();
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            logger.error("Interrupted during network update", e);
            return;
        }

        Set<Integer> activeIds;
        try {
            networkLock.lock();
            try {
                activeIds = new HashSet<>(knownPlants.keySet());
            } finally {
                networkLock.unlock();
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return;
        }

        removeObsoleteStubs(activeIds);
        startGRPCClient();
    }

    /**
     *  Thread that periodically refreshes network snapshot using updateNetwork(); handles errors
     * **/
    private void startNetworkRefresher() {
        Thread refresher = new Thread(() -> {
            logger.info("Starting network refresher thread");
            while (!stopFlag) {

                try {
                    List<Plant> snapshot = restClient.getNetwork();
                    if (snapshot != null) {
                        updateNetwork(snapshot);
                    }
                    Thread.sleep(5000);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    logger.error("Error during network refresh", e);
                    return;
                } catch (Exception e) {
                    logger.error("Error during network refresh", e);
                }
            }
        });

        refresher.setDaemon(true);
        refresher.start();
    }

    private void startGRPCServer() throws Exception {
        Server server = ServerBuilder.forPort(portNumber)
                .addService(new PlantCoordinationServiceImpl(this))
                .build()
                .start();

        logger.info("gRPC server started on port {}", portNumber);
    }

    /**
     * Creates gRPC clients for known plants.
     * My implementation of the "presentation" of a new plant to the network, united with the method updateNetwork().
     */
    private void startGRPCClient() {
        try {
            networkLock.lock();
            try {
                for (Plant p : knownPlants.values()) {
                    int pid = p.getId();
                    if (stubs.containsKey(pid)) {
                        continue;
                    }
                    ManagedChannel channel =
                            ManagedChannelBuilder
                                    .forAddress("localhost", p.getPort())
                                    .usePlaintext()
                                    .build();

                    PlantCoordinationServiceGrpc.PlantCoordinationServiceBlockingStub stub =
                            PlantCoordinationServiceGrpc.newBlockingStub(channel);

                    stubs.put(pid, stub);
                    logger.info("gRPC stub created for plant {}", pid);
                }
            } finally {
                networkLock.unlock();
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            logger.error("Interrupted while creating gRPC clients", e);
        }
    }

    private void enqueueMqttTask(Runnable task) {
        synchronized (mqttQueue) {
            mqttQueue.add(task);
            mqttQueue.notify();
        }
    }

    private void startMqttOutboundThread() {
        Thread t = new Thread(() -> {
            while (!stopFlag) {
                Runnable task;
                synchronized (mqttQueue) {
                    while (mqttQueue.isEmpty()) {
                        try {
                            mqttQueue.wait();
                        } catch (InterruptedException e) {
                            return;
                        }
                    }
                    task = mqttQueue.removeFirst();
                }
                try {
                    task.run();
                } catch (Exception e) {
                    logger.error("MQTT outbound task failed", e);
                }
            }
        });

        t.start();
    }

    private void safePublish(String topic, MqttMessage msg) {
        enqueueMqttTask(() -> {
            try {
                mqttClient.publish(topic, msg);
            } catch (MqttException e) {
                logger.error("MQTT publish failed on topic {}", topic, e);
            }
        });
    }

    private void startPollutionPublisherThread() {
        Thread t = new Thread(() -> {
            while (!stopFlag) {
                try {
                    List<Measurement> window = buffer.extractWindowIfAvailable();
                    if (window == null) {
                        Thread.sleep(500);
                        continue;
                    }

                    AverageMeasurement avg =
                            AverageMeasurement.from(window, id);

                    String payload =
                            AverageMeasurementMapper.serialize(avg);

                    MqttMessage msg =
                            new MqttMessage(payload.getBytes());
                    msg.setQos(1);
                    msg.setRetained(false);

                    safePublish(POLLUTION_TOPIC, msg);

                } catch (Exception e) {
                    logger.error("Error publishing pollution averages", e);
                }
            }
        });

        t.setDaemon(true);
        t.start();
    }

    private void startHeartbeatThread() {
        new Thread(() -> {
            while (!stopFlag) {
                try {
                    restClient.sendHeartbeat(id);
                    Thread.sleep(5_000);
                } catch (Exception e) {
                    logger.warn("Heartbeat failed", e);
                }
            }
        }).start();
    }

    private void connectMQTT() throws MqttException {
        mqttClient = new MqttClient("tcp://localhost:1883", "TPP-" + id);
        mqttClient.connect();

        mqttClient.subscribe("energy/requests", (topic, message) -> {
            if (message.getPayload().length == 0) {
                return; // retained clear
            }
            EnergyRequest req =
                    EnergyRequestMapper.deserialize(new String(message.getPayload()));
            pendingRequests.put(req.getTimestamp(), req);
            startElection(req);
        });


        logger.info("Connected to MQTT broker");
    }

    private void clearEnergyRequestFromTopic() {
        try {
            MqttMessage msg = new MqttMessage(new byte[0]) {{
                setQos(1);
                setRetained(true);
            }};
            safePublish(ER_TOPIC, msg);

            logger.info("[MQTT] Cleared energy request from topic");

        } catch (Exception e) {
            logger.error("Failed to clear MQTT energy request", e);
        }
    }

    private void startElection(EnergyRequest req) {

        long requestId = req.getTimestamp();
        ElectionContext ctx;

        try {
            electionLock.lock();
            try {
                if (electionContexts.containsKey(requestId)) {
                    return;
                }

                float myBid = generateBid();
                ctx = new ElectionContext(requestId, myBid);
                ctx.setParticipant(true);
                ctx.setBestBid(myBid);
                ctx.setBestCandidateId(this.id);

                electionContexts.put(requestId, ctx);

                logger.info(
                        "[ELECTION-START] plant={} request={} bid={}",
                        id, requestId, myBid
                );

            } finally {
                electionLock.unlock();
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return;
        }

        int successorId = retrieveSuccessorId();
        // solo una plant → vittoria immediata
        if (successorId == this.id) {
            startLeaderElectedPhase(requestId, this.id);
            return;
        }

        ElectionMessage msg = ElectionMessage.newBuilder()
                .setRequestId(requestId)
                .setCandidateId(this.id)
                .setBid(ctx.getMyBid())
                .build();

        try {
            PlantCoordinationServiceBlockingStub stub = stubs.get(successorId);
            if (stub == null) {
                restartElection(requestId);
                return;
            }
            stub.election(msg);
        } catch (Exception e) {
            restartElection(requestId);
        }
    }

    public void onElectionMessage(ElectionMessage msg) {

        long requestId = msg.getRequestId();
        int incomingCandidate = msg.getCandidateId();
        float incomingBid = msg.getBid();

        ElectionContext ctx;
        boolean iAmLeader = false;
        int forwardCandidate;
        float forwardBid;

        try {
            electionLock.lock();
            try {
                ctx = electionContexts.get(requestId);
                if (ctx == null || ctx.isCompleted()) {
                    return;
                }

                // ritorno del mio ID → sono il leader
                if (incomingCandidate == this.id) {
                    ctx.setCompleted(true);
                    iAmLeader = true;
                    logger.info(
                            "[ELECTION-WIN] plant={} request={} bid={}",
                            id, requestId, ctx.getBestBid()
                    );
                } else {
                    boolean better =
                            incomingBid < ctx.getBestBid() ||
                                    (incomingBid == ctx.getBestBid()
                                            && incomingCandidate > ctx.getBestCandidateId());

                    if (better) {
                        ctx.setBestBid(incomingBid);
                        ctx.setBestCandidateId(incomingCandidate);
                    }

                    ctx.setParticipant(true);
                }

                forwardCandidate = ctx.getBestCandidateId();
                forwardBid = ctx.getBestBid();

            } finally {
                electionLock.unlock();
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return;
        }

        if (iAmLeader) {
            startLeaderElectedPhase(requestId, this.id);
            return;
        }

        int successorId = retrieveSuccessorId();

        ElectionMessage forward = ElectionMessage.newBuilder()
                .setRequestId(requestId)
                .setCandidateId(forwardCandidate)
                .setBid(forwardBid)
                .build();

        try {
            PlantCoordinationServiceBlockingStub stub = stubs.get(successorId);
            if (stub == null) {
                restartElection(requestId);
                return;
            }
            stub.election(forward);
        } catch (Exception e) {
            restartElection(requestId);
        }
    }

    private void startLeaderElectedPhase(long requestId, int leaderId) {

        int successorId = retrieveSuccessorId();
        if (successorId == this.id) {
            // solo nodo → fine immediata
            handleElectionWin(requestId);
            return;
        }

        WinnerMessage msg = WinnerMessage.newBuilder()
                .setRequestId(requestId)
                .setWinnerId(leaderId)
                .build();

        try {
            stubs.get(successorId).leaderElected(msg);
        } catch (Exception e) {
            restartElection(requestId);
        }
    }

    public void onLeaderElected(long requestId, int winnerId) {

        boolean finalReturn = false;

        try {
            electionLock.lock();
            try {
                ElectionContext ctx = electionContexts.get(requestId);
                if (ctx == null) return;

                if (winnerId == this.id && ctx.isParticipant()) {
                    electionContexts.remove(requestId);
                    finalReturn = true;
                } else {
                    ctx.setCompleted(true);
                }
            } finally {
                electionLock.unlock();
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return;
        }

        if (!finalReturn) {
            int successorId = retrieveSuccessorId();
            if (successorId != this.id) {
                WinnerMessage msg = WinnerMessage.newBuilder()
                        .setRequestId(requestId)
                        .setWinnerId(winnerId)
                        .build();
                try {
                    stubs.get(successorId).leaderElected(msg);
                } catch (Exception ignored) {}
            }
            return;
        }

        // SOLO ORA il leader produce
        handleElectionWin(requestId);
    }

    /**
     * Handles election win; attempts atomic production commit
     */
    public void handleElectionWin(long requestId) {

        EnergyRequest request = pendingRequests.get(requestId);
        if (request == null) return;

        if (!plantState.tryAcquire(requestId)) {
            restartElection(requestId);
            return;
        }
        logger.info("[ELECTION-COMMIT] plant={} request={}", id, requestId);

        clearEnergyRequestFromTopic();
        pendingRequests.remove(requestId);

        new Thread(() -> produceEnergy(request)).start();
    }

    /**
     * Used when a new plant joins the network during an election process
     * **/
    private void restartElection(long requestId) {

        EnergyRequest req;

        try {
            electionLock.lock();
            try {
                ElectionContext old = electionContexts.remove(requestId);
                if (old != null) {
                    logger.warn(
                            "[ELECTION-RESTART] cleaning context for request {}",
                            requestId
                    );
                }
            } finally {
                electionLock.unlock();
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return;
        }

        req = pendingRequests.get(requestId);
        if (req == null) {
            logger.warn(
                    "[ELECTION-RESTART-IGNORED] request {} no longer pending",
                    requestId
            );
            return;
        }

        logger.info(
                "[ELECTION-RESTART] plant={} request={}",
                id, requestId
        );

        startElection(req);
    }

    public void start() throws Exception {
        logger.info("Starting ThermalPowerPlant {}", id);

        restClient = new RestClient(REST_SERVER_ADDRESS, REST_SERVER_PORT);

        Plant plant = new Plant(id, portNumber);
        List<Plant> initialNetwork = restClient.registerPlantAndGetNetwork(plant);
        initializeNetwork(initialNetwork);
        startGRPCServer();
        startNetworkRefresher();
        startGRPCClient();

        buffer = new SlidingWindowBuffer();
        sensor = new PollutionSensor(buffer);
        sensor.start();
        logger.info("Pollution sensor started");

        startHeartbeatThread();

        connectMQTT();
        startMqttOutboundThread();
        startPollutionPublisherThread();

        startCommandListener();
    }

    private void produceEnergy(EnergyRequest request) {
        try {
            Thread.sleep((long) request.getAmount());
            logger.info("[PRODUCTION-END] plant={} request={}", id, request.getTimestamp());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            logger.error("Interrupted during energy production", e);
        } finally {
            plantState.release(request.getTimestamp());
        }
    }

    public void handleRemoteExit(int pid) {
        try {
            networkLock.lock();
            try {
                knownPlants.remove(pid);
                stubs.remove(pid);
                logger.info("Handled remote exit of plant {}", pid);
            } finally {
                networkLock.unlock();
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            logger.error("Interrupted while handling remote exit of plant {}", pid, e);
        }

        // abort election?
    }

    public void stop() {
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
        for (PlantCoordinationServiceBlockingStub stub : stubs.values()) {
            stub.notifyExit(ExitNotification.newBuilder()
                    .setPlantId(id)
                    .build());
        }

        restClient.removePlant(this.id);
        logger.info("ThermalPowerPlant {} stopped", id);
    }

    private void startCommandListener() {
        new Thread(() -> {
            try (Scanner sc = new Scanner(System.in)) {
                while (!stopFlag) {
                    String cmd = sc.nextLine();
                    if ("exit".equalsIgnoreCase(cmd.trim())) {
                        stop();
                        break;
                    }
                }
            }
        }).start();
    }

    public static void main(String[] args) {
        try {
            int id = insertID();
            ThermalPowerPlant plant = new ThermalPowerPlant(id);

            plant.start();
            Thread.sleep(60000);
            plant.stop();

        } catch (Exception e) {
            LoggerFactory.getLogger(ThermalPowerPlant.class)
                    .error("Fatal error in ThermalPowerPlant main", e);
        }
    }
}
