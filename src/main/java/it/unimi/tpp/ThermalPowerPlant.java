package it.unimi.tpp;

import it.unimi.adminclient.RestClient;
import it.unimi.adminserver.model.Plant;
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
import java.util.*;

import static java.lang.Thread.sleep;

public class ThermalPowerPlant {

    private static final Logger logger =
            LoggerFactory.getLogger(ThermalPowerPlant.class);
    private static final Scanner STDIN = new Scanner(System.in);


    private final int id;
    private final int portNumber;
    private final ThermalPlantState plantState = new ThermalPlantState();
    private SlidingWindowBuffer buffer;

    private final MyLock electionLock = new MyLock();
    private final Queue<EnergyRequest> pendingRequests = new LinkedList<>();
    private final Map<Long,ElectionContext> electionContexts = new HashMap<>();
    private final Set<Long> handledRequests = new HashSet<>();

    public static final String REST_SERVER_ADDRESS = "localhost";
    public static final int REST_SERVER_PORT = 8080;
    private static final String ER_TOPIC = "energy/requests";
    private static final String POLLUTION_TOPIC = "pollution/averages";

    private PollutionSensor sensor;

    private RestClient restClient;

    private MqttClient mqttClient;
    private final LinkedList<Runnable> mqttQueue = new LinkedList<>();


    private final MyLock networkLock = new MyLock();
    private final Map<Integer, Plant> knownPlants = new HashMap<>();
    private final Map<Integer, PlantCoordinationServiceBlockingStub> stubs =
            new HashMap<>();

    private volatile boolean stopFlag = false;

    public ThermalPowerPlant(int id) {
        this.id = id;
        this.portNumber = 5000 + id;
    }

    /**
     * Determines ID of the next plant to recreate a ring topology used by the Chang&Roberts algorithm.
     */
    private int retrieveSuccessorId(ElectionContext ctx) {

        List<Integer> ids = ctx.getElectionView();

        int index = ids.indexOf(this.id);
        if (index == -1) {
            throw new IllegalStateException(
                    "Current plant not present in election view");
        }

        return ids.get((index + 1) % ids.size());
    }


    private float generateBid() {
        return 0.1f + (float) Math.random() * (0.9f - 0.1f);
    }

    private static int insertID() {
        RestClient restClient = new RestClient(REST_SERVER_ADDRESS, REST_SERVER_PORT);


        while (true) {
            System.out.print("Enter plant ID (>0): ");
            int id = STDIN.nextInt();

            if (id <= 0) {
                System.out.println("ID must be > 0");
                continue;
            }

            List<Plant> network = restClient.getNetwork();
            for (Plant p : network) {
                if (p.getId() == id) {
                    System.out.println("ID already in use by an active plant");
                    id = -1;
                    break;
                }
            }

            if (id > 0) {
                STDIN.nextLine();
                return id;
            }
        }
    }

    /**
     * Updates known plants; logs active sets; guards against interruption
     * **/
    private void initializeNetwork(List<Plant> network) {
        try {
            networkLock.lock();

                knownPlants.clear();
                for (Plant p : network) {
                    if (p.getId() != this.id) {
                        knownPlants.put(p.getId(), p);
                    }
                }
            logger.info("Initial network view: {}", knownPlants.keySet());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            logger.error("Interrupted during network initialization", e);
        } finally {networkLock.unlock();}
    }

    private void announceJoin() {

        JoinNotification msg = JoinNotification.newBuilder()
                .setPlantId(id)
                .setPort(portNumber)
                .build();

        try {
            networkLock.lock();
            try {
                for (Map.Entry<Integer, PlantCoordinationServiceBlockingStub> entry : stubs.entrySet()) {
                    int targetId = entry.getKey();
                    PlantCoordinationServiceBlockingStub stub = entry.getValue();

                    try {
                        stub.notifyJoin(msg);
                        logger.info("Join sent to plant {}", targetId);
                    } catch (Exception e) {
                        logger.warn("Failed to send JOIN to plant {}", targetId);
                    }
                }
            } finally {
                networkLock.unlock();
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private void announceExit() {

        ExitNotification msg = ExitNotification.newBuilder()
                .setPlantId(id)
                .build();

        try {
            networkLock.lock();
            try {
                for (Map.Entry<Integer, PlantCoordinationServiceBlockingStub> entry : stubs.entrySet()) {
                    int targetId = entry.getKey();
                    PlantCoordinationServiceBlockingStub stub = entry.getValue();

                    try {
                        stub.notifyExit(msg);
                        logger.info("Exit sent to plant {}", targetId);
                    } catch (Exception e) {
                        logger.warn("Failed to send EXIT to plant {}", targetId);
                    }
                }
            } finally {
                networkLock.unlock();
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    public void handleRemoteJoin(int pid, int port) {

        List<Plant> targets = new ArrayList<>();

        try {
            networkLock.lock();
            try {
                if (pid == this.id || knownPlants.containsKey(pid)) {
                    return; // JOIN già visto
                }

                Plant p = new Plant(pid, port);
                knownPlants.put(pid, p);
                logger.info("Plant {} added to local view", pid);


                for (Plant other : knownPlants.values()) {
                    if (other.getId() != pid) {
                        targets.add(other);
                    }
                }
            } finally {
                networkLock.unlock();
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return;
        }

        // crea stub per il nuovo peer
        createStubIfAbsent(pid, port);

        // flooding guidato dalla VISTA LOGICA
        for (Plant target : targets) {
            createStubIfAbsent(target.getId(), target.getPort());
            try {
                stubs.get(target.getId()).notifyJoin(
                        JoinNotification.newBuilder()
                                .setPlantId(pid)
                                .setPort(port)
                                .build()
                );
            } catch (Exception e) {
                logger.warn("Failed to propagate JOIN to {}", target.getId());
            }
        }
    }


    private void propagateExit(int pid) {

        ExitNotification msg = ExitNotification.newBuilder()
                .setPlantId(pid)
                .build();

        try {
            networkLock.lock();
            try {
                for (var stub : stubs.values()) {
                    try {
                        stub.notifyExit(msg);
                    } catch (Exception ignored) {}
                }
            } finally {
                networkLock.unlock();
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    public void handleRemoteExit(int pid) {

        boolean removed = false;

        try {
            networkLock.lock();
            try {
                if (knownPlants.containsKey(pid)) {
                    knownPlants.remove(pid);
                    stubs.remove(pid);
                    removed = true;
                    logger.info("Plant {} removed from local view", pid);
                }
            } finally {
                networkLock.unlock();
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return;
        }
        if (!removed) {
            return;
        }

        propagateExit(pid);
    }

    private void createStubIfAbsent(int pid, int port) {
        try {
            networkLock.lock();
            try {
                if (stubs.containsKey(pid)) return;

                ManagedChannel channel =
                        ManagedChannelBuilder.forAddress("localhost", port)
                                .usePlaintext()
                                .build();

                var stub =
                        PlantCoordinationServiceGrpc.newBlockingStub(channel);

                stubs.put(pid, stub);

                logger.info("Created gRPC stub for plant {}", pid);
            } finally {
                networkLock.unlock();
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
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
                        sleep(500);
                        continue;
                    }

                    AverageMeasurement avg =
                            AverageMeasurement.from(window, id);

                    String payload =
                            AverageMeasurementMapper.serialize(avg);

                    MqttMessage msg =
                            new MqttMessage(payload.getBytes());
                    msg.setQos(0);
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
                    sleep(5000);
                } catch (Exception e) {
                    logger.warn("Heartbeat failed", e);
                }
            }
        }).start();
    }

    private void connectMQTTSubscriberThread() throws MqttException {
        mqttClient = new MqttClient("tcp://localhost:1883", "TPP-" + id);
        mqttClient.connect();

        mqttClient.subscribe(ER_TOPIC, (topic, message) -> {

            // retained clear
            if (message.getPayload().length == 0) {
                return;
            }

            EnergyRequest req =
                    EnergyRequestMapper.deserialize(new String(message.getPayload()));

            synchronized (electionLock) {
                pendingRequests.add(req);
                electionLock.notifyAll();
            }

        });

        logger.info("Connected to MQTT broker");
    }

    private void startElectionThread() {
        Thread t = new Thread(() -> {
            while (!stopFlag) {
                EnergyRequest req;

                synchronized (electionLock) {
                    while (pendingRequests.isEmpty()) {
                        try {
                            electionLock.wait();
                        } catch (InterruptedException e) {
                            return;
                        }
                    }

                    req = pendingRequests.poll();
                }

                synchronized (this) {
                    if (handledRequests.contains(req.getTimestamp())) {
                        continue;
                    }
                }

                if (plantState.isBusy()) {
                    logger.info(
                            "[ENERGY-REQUEST] plant={} BUSY → request {} discarded",
                            id, req.getTimestamp()
                    );
                    continue;
                }

                boolean shouldStart = false;
                synchronized (this) {
                    if (!handledRequests.contains(req.getTimestamp())
                            && !electionContexts.containsKey(req.getTimestamp())) {
                        shouldStart = true;
                    }
                }

                if (shouldStart) {
                    startElection(req);
                }
            }
        });

        t.setDaemon(true);
        t.start();
    }

    private void startElection(EnergyRequest req) {

        logger.info("[ELECTION-START] plant={} request={}", id, req.getTimestamp());

        float myBid = generateBid();
        ElectionContext ctx;
        PlantCoordinationServiceBlockingStub nextStub;

        synchronized (this) {

            // 1. costruzione SNAPSHOT
            List<Integer> electionView = new ArrayList<>(knownPlants.keySet());
            electionView.add(this.id);
            Collections.sort(electionView);

            // 2. creazione contesto
            ctx = new ElectionContext(req, myBid, electionView);
            ctx.setParticipant(true);
            electionContexts.put(req.getTimestamp(), ctx);

            // 3. calcolo successore sulla SNAPSHOT
            int successorId = retrieveSuccessorId(ctx);
            nextStub = stubs.get(successorId);
        }

        forwardElection(nextStub, req.getTimestamp(), id, myBid);
    }


    public void onElectionMessage(long requestId, int candidateId, float candidateBid) {

        ElectionContext ctx;
        PlantCoordinationServiceBlockingStub nextStub;

        synchronized (this) {
            ctx = electionContexts.get(requestId);
            if (ctx == null) {
                return;
            }
            int successorId = retrieveSuccessorId(ctx);
            nextStub = stubs.get(successorId);
        }

        if (plantState.isBusy()) {
            forwardElection(nextStub, requestId, candidateId, candidateBid);
            return;
        }

        if (id == candidateId) {
            ctx.setParticipant(false);
            forwardElected(nextStub, requestId, id);
        } else if (candidateBid < ctx.getMyBid()
                || (candidateBid == ctx.getMyBid() && candidateId > id)) {
            ctx.setParticipant(true);
            forwardElection(nextStub, requestId, candidateId, candidateBid);
        } else if (!ctx.isParticipant()) {
            ctx.setParticipant(true);
            forwardElection(nextStub, requestId, id, ctx.getMyBid());
        }
    }


    private void forwardElection(PlantCoordinationServiceBlockingStub stub, long requestId, int candidateId, float bid) {
        ElectionMessage msg = ElectionMessage.newBuilder()
                .setRequestId(requestId)
                .setCandidateId(candidateId)
                .setBid(bid)
                .build();
        stub.election(msg);
    }

    public void forwardElected(PlantCoordinationServiceBlockingStub stub, long requestId, int winnerId) {
        ElectedMessage msg = ElectedMessage.newBuilder()
                .setRequestId(requestId)
                .setWinnerId(winnerId)
                .build();
        stub.elected(msg);
    }

    public void onElected(long requestId, int winnerId) {

        ElectionContext ctx;
        PlantCoordinationServiceBlockingStub nextStub;

        synchronized (this) {
            ctx = electionContexts.get(requestId);
            if (ctx == null) {
                return;
            }

            int successorId = retrieveSuccessorId(ctx);
            nextStub = stubs.get(successorId);
            handledRequests.add(requestId);
        }

        if (plantState.isBusy()) {
            forwardElected(nextStub, requestId, winnerId);
            return;
        }

        if (winnerId == id) {
            handleWin(requestId);
        } else {
            ctx.setParticipant(false);
            forwardElected(nextStub, requestId, winnerId);
        }
    }

    private void handleWin(long requestId) {
    logger.info("[ELECTION-WIN] plant={} request={}", id, requestId);
    cancelEnergyRequestOnMQTT();
    EnergyRequest req;
    synchronized (this) {
        ElectionContext ctx = electionContexts.get(requestId);


        if (ctx == null) {
            logger.warn("Election context already processed for request {}", requestId);
            return;
        }

        req = ctx.getRequest();

        if (!plantState.tryAcquire(req.getTimestamp())) {
            logger.warn("Plant {} already busy, cannot handle win for request {}", id, requestId);
            electionContexts.remove(requestId);
            return;
        }

        electionContexts.remove(requestId);
    }

    produceEnergy(req);
}

    private void cancelEnergyRequestOnMQTT() {
        try {
            MqttMessage empty = new MqttMessage(new byte[0]);
            empty.setQos(1);
            empty.setRetained(true);

            mqttClient.publish(ER_TOPIC, empty);
            logger.info("Cancelled retained energy request on topic {}", ER_TOPIC);
        } catch (Exception e) {
            logger.error("Failed to cancel retained energy request", e);
        }
    }

    public void start() throws Exception {
        logger.info("Starting ThermalPowerPlant {}", id);

        restClient = new RestClient(REST_SERVER_ADDRESS, REST_SERVER_PORT);

        Plant plant = new Plant(id, portNumber);
        List<Plant> initialNetwork = restClient.registerPlantAndGetNetwork(plant);

        initializeNetwork(initialNetwork);

        startGRPCServer();
        startGRPCClient();

        if (!initialNetwork.isEmpty()) {
            announceJoin();
        }

        buffer = new SlidingWindowBuffer();
        sensor = new PollutionSensor(buffer);
        sensor.start();
        logger.info("Pollution sensor started");

        //startHeartbeatThread();

        startElectionThread();
        connectMQTTSubscriberThread();
        startMqttOutboundThread();
        startPollutionPublisherThread();

        startCommandListener();
    }

    private void produceEnergy(EnergyRequest request) {
        try {
            logger.info("[PRODUCTION-START] plant={} request={}", id, request.getTimestamp());
            sleep((long) request.getAmount());
            logger.info("[PRODUCTION-END] plant={} request={}", id, request.getTimestamp());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            logger.error("Interrupted during energy production", e);
        } finally {
            plantState.release(request.getTimestamp());
        }
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

        announceExit();

        restClient.removePlant(this.id);
        logger.info("ThermalPowerPlant {} stopped", id);
    }

    private void startCommandListener() {
        Thread t = new Thread(() -> {

            while (!stopFlag) {
                try {
                    String cmd = STDIN.nextLine();
                    if ("exit".equalsIgnoreCase(cmd.trim())) {
                        stop();
                        break;
                    }
                } catch (Exception e) {
                    logger.error("Error reading command", e);
                    break;
                }
            }
        });

        t.setDaemon(true);
        t.start();
    }


    public static void main(String[] args) throws Exception {

            int id = insertID();
            ThermalPowerPlant plant = new ThermalPowerPlant(id);

            plant.start();

    }
}
