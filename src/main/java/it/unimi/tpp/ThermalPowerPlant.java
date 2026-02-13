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
    private final MyLock stateLock = new MyLock();
    private final Queue<EnergyRequest> pendingRequests = new LinkedList<>();
    private final Map<Long,ElectionContext> electionsInProgress = new HashMap<>();
    private final Map<Long,ElectionContext> electionsCompleted = new HashMap<>();

    public static final String REST_SERVER_ADDRESS = "localhost";
    public static final int REST_SERVER_PORT = 8080;
    private static final String ER_TOPIC = "energy/requests";
    private static final String POLLUTION_TOPIC = "pollution/averages";
    private static final boolean DEBUG_SLOW_ELECTION = true;

    private PollutionSensor sensor;

    private RestClient restClient;

    private MqttClient mqttClient;
    private Server grpcServer;
    private final LinkedList<Runnable> mqttQueue = new LinkedList<>();

    private final MyLock networkLock = new MyLock();
    private final Map<Integer, Plant> knownPlants = new HashMap<>();
    private final Map<Integer, PlantCoordinationServiceBlockingStub> stubs =
            new HashMap<>();

    private volatile boolean exitInProgress = false;
    private volatile boolean stopFlag = false;

    private Thread mqttOutboundThread;
    private Thread heartbeatThread;

    public ThermalPowerPlant(int id) {
        this.id = id;
        this.portNumber = 5000 + id;
    }

    public int getId() {
        return id;
    }

    public int getPortNumber() {return portNumber;}

    private int retrieveSuccessorId(int myId, List<Integer> ringOrder) {

        int index = ringOrder.indexOf(myId);
        if (index == -1)
            throw new IllegalStateException();

        return ringOrder.get((index + 1) % ringOrder.size());
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

    private void setKnownPlants(List<Plant> network) {
        try {
            networkLock.lock();
            knownPlants.clear();
                for (Plant p : network) {
                        knownPlants.put(p.getId(), p);
                }
            logger.info("Initial network view: {}", knownPlants.keySet());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            logger.error("Interrupted during network initialization", e);
        } finally {networkLock.unlock();}
    }

    private void announceJoin(List<Plant> initialNetwork) {

        JoinNotification msg = JoinNotification.newBuilder()
                .setPlantId(id)
                .setPort(portNumber)
                .build();

        for (Plant p : initialNetwork) {
                try {
                    ManagedChannel channel =
                            ManagedChannelBuilder.forAddress("localhost", p.getPort())
                                    .usePlaintext()
                                    .build();

                    PlantCoordinationServiceBlockingStub stub =
                            PlantCoordinationServiceGrpc.newBlockingStub(channel);

                    JoinAck ack = stub.notifyJoin(msg);

                    createStubIfAbsent(ack.getPlantId(), ack.getPort());

                    logger.info(
                            "[JOIN-ACK] received from plant {}",
                            ack.getPlantId()
                    );

                } catch (Exception e) {
                    logger.warn("JOIN failed towards plant {}", p.getId(), e);
                }
        }
    }

    public void handleRemoteJoin(int pid, int port) {

        try {
            networkLock.lock();

            Plant p = new Plant(pid, port);
            knownPlants.put(pid, p);

            logger.info(
                    "[JOIN-LOCAL] plant {} added to local view",
                    pid
            );

        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return;
        } finally {
            networkLock.unlock();
        }

        createStubIfAbsent(pid, port);
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

    public void initiateExit() {

        exitInProgress = true;

        abortAllElectionsDueToTopologyChange();

        if (plantState.isBusy()) {
            logger.info("[EXIT-DEFERRED] plant {} busy, waiting for IDLE", id);
            return;
        }

        completeExitProcedure();
    }

    public void handleRemoteExit(int exitingPlantId) {

        try {
            networkLock.lock();
            knownPlants.remove(exitingPlantId);
            stubs.remove(exitingPlantId);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return;
        } finally {
            networkLock.unlock();
        }

        abortAllElectionsDueToTopologyChange();
    }

    private void abortAllElectionsDueToTopologyChange() {

        List<EnergyRequest> toRestart = new ArrayList<>();
        boolean hadActiveElections = false;

        try {
            stateLock.lock();

            if (!electionsInProgress.isEmpty()) {
                hadActiveElections = true;

                for (ElectionContext ctx : electionsInProgress.values()) {
                    toRestart.add(ctx.getRequest());
                }

                electionsInProgress.clear();
                electionsCompleted.clear();
            }

        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return;
        } finally {
            stateLock.unlock();
        }

        if (!hadActiveElections) {
            return;
        }

        try {
            electionLock.lock();
            pendingRequests.addAll(toRestart);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } finally {
            electionLock.unlock();
        }

        logger.info("[ABORT] active elections invalidated due to topology change");
    }

    private void completeExitProcedure() {

        List<Integer> targets;
        Map<Integer, PlantCoordinationServiceBlockingStub> stubsCopy;

        try {
            networkLock.lock();
            targets = new ArrayList<>(knownPlants.keySet());
            stubsCopy = new HashMap<>(stubs);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return;
        } finally {
            networkLock.unlock();
        }

        ExitNotification msg = ExitNotification.newBuilder()
                .setPlantId(id)
                .build();

        for (int pid : targets) {
            PlantCoordinationServiceBlockingStub stub = stubsCopy.get(pid);
            if (stub == null) continue;

            try {
                stub.notifyExit(msg);
            } catch (Exception ignored) {}

        }

        stop();
    }

    private void startGRPCServer() throws Exception {
        grpcServer = ServerBuilder.forPort(portNumber)
                .addService(new PlantCoordinationServiceImpl(this))
                .build()
                .start();

        logger.info("gRPC server started on port {}", portNumber);
    }

    private void enqueueMqttTask(Runnable task) {
        synchronized (mqttQueue) {
            mqttQueue.add(task);
            mqttQueue.notify();
        }
    }

    private void startMqttOutboundThread() {
        mqttOutboundThread = new Thread(() -> {
            while (!stopFlag) {
                Runnable task;
                synchronized (mqttQueue) {
                    while (mqttQueue.isEmpty() && !stopFlag) {
                        try {
                            mqttQueue.wait();
                        } catch (InterruptedException e) {
                            return;
                        }
                    }
                    if (stopFlag) return;
                    task = mqttQueue.removeFirst();
                }
                try {
                    task.run();
                } catch (Exception e) {
                    logger.error("MQTT outbound task failed", e);
                }
            }
        });

        mqttOutboundThread.start();
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
        Thread pollutionPublisherThread = new Thread(() -> {
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

        pollutionPublisherThread.setDaemon(true);
        pollutionPublisherThread.start();
    }

    private void startHeartbeatThread() {
        heartbeatThread = new Thread(() -> {
            while (!stopFlag) {
                try {
                    restClient.sendHeartbeat(id);
                    sleep(2000);
                } catch (Exception e) {
                    logger.warn("Heartbeat failed", e);
                }
            }
        });
        heartbeatThread.setDaemon(true);
        heartbeatThread.start();
    }

    private void connectMQTTSubscriberThread() throws MqttException {
        mqttClient = new MqttClient("tcp://localhost:1883", "TPP-" + id);
        mqttClient.connect();

        mqttClient.subscribe(ER_TOPIC, (topic, message) -> {


            if (message.getPayload().length == 0) {
                return;
            }

            EnergyRequest req =
                    EnergyRequestMapper.deserialize(new String(message.getPayload()));

            try {
                electionLock.lock();
                pendingRequests.add(req);
            } finally {
                electionLock.unlock();
            }



        });

        logger.info("Connected to MQTT broker");
    }

    private void startElectionThread() {
        Thread electionThread = new Thread(() -> {
            while (!stopFlag) {

                EnergyRequest req = null;

                try {
                    electionLock.lock();
                    if (!pendingRequests.isEmpty()) {
                        req = pendingRequests.poll();
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    return;
                } finally {
                    electionLock.unlock();
                }

                if (req == null) {
                    continue;
                }

                boolean shouldStart = true;

                try {
                    stateLock.lock();

                    if (electionsInProgress.containsKey(req.getTimestamp()) ||
                            electionsCompleted.containsKey(req.getTimestamp())) {
                        shouldStart = false;
                    }

                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    return;
                } finally {
                    stateLock.unlock();
                }

                if (!exitInProgress && shouldStart && !plantState.isBusy())
                    startElection(req);

            }
        });

        electionThread.setDaemon(true);
        electionThread.start();
    }

    private void startElection(EnergyRequest req) {

        if (plantState.isBusy()) {
            return;
        }

        logger.info("[ELECTION-START] plant={} request={}", id, req.getTimestamp());

        float myBid = generateBid();
        List<Integer> ids;
        Map<Integer, PlantCoordinationServiceBlockingStub> ring;
        PlantCoordinationServiceBlockingStub nextStub;
        ElectionContext ctx;

        try {
            networkLock.lock();
            ids = new ArrayList<>(knownPlants.keySet());
            ids.add(this.id);
            Collections.sort(ids);
            ring = new HashMap<>(stubs);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return;
        } finally {
            networkLock.unlock();
        }

        try {
            stateLock.lock();
            ctx = new ElectionContext(ids, ring, req, myBid, true);
            electionsInProgress.put(req.getTimestamp(), ctx);
            nextStub = ctx.getRingStubs()
                    .get(retrieveSuccessorId(this.id, ids));
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return;
        } finally {
            stateLock.unlock();
        }

        forwardElection(nextStub, req.getTimestamp(), id, myBid);
    }

    public void onElectionMessage(long requestId, int candidateId, float candidateBid) {
        if (exitInProgress) {
            return;
        }

        ElectionContext ctx;
        PlantCoordinationServiceBlockingStub nextStub;

        try {
            stateLock.lock();
            ctx = electionsInProgress.get(requestId);

            if (ctx == null) {

                List<Integer> ids = new ArrayList<>(knownPlants.keySet());
                ids.add(this.id);
                Collections.sort(ids);

                Map<Integer, PlantCoordinationServiceBlockingStub> ring =
                        new HashMap<>(stubs);

                float myBid = generateBid();

                boolean participant = !plantState.isBusy();

                ctx = new ElectionContext(ids, ring,
                        new EnergyRequest(requestId, 0),
                        myBid,
                        participant);

                electionsInProgress.put(requestId, ctx);
            }

            nextStub = ctx.getRingStubs()
                    .get(retrieveSuccessorId(this.id, ctx.getRingOrder()));

        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return;
        } finally {
            stateLock.unlock();
        }

        if (plantState.isBusy()) {
            forwardElection(nextStub, requestId, candidateId, candidateBid);
            return;
        }

        if (id == candidateId) {
            ctx.setParticipant(false);
            forwardElected(nextStub, requestId, id);
        } else if (candidateBid < ctx.getMyBid() ||
                (candidateBid == ctx.getMyBid() && candidateId > id)) {
            forwardElection(nextStub, requestId, candidateId, candidateBid);
        } else if (!ctx.isParticipant()) {
            forwardElection(nextStub, requestId, id, ctx.getMyBid());
        }
    }

    private void forwardElection(
            PlantCoordinationServiceBlockingStub stub,
            long requestId,
            int candidateId,
            float bid) {

        ElectionMessage msg = ElectionMessage.newBuilder()
                .setRequestId(requestId)
                .setCandidateId(candidateId)
                .setBid(bid)
                .build();

        if (DEBUG_SLOW_ELECTION) {
            try {
                Thread.sleep(500);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }

        try {
            stub.election(msg);
        } catch (StatusRuntimeException e) {
            logger.debug("[RPC-FAIL] election forward failed: {}", e.getStatus());
        }
    }


    public void forwardElected(
            PlantCoordinationServiceBlockingStub stub,
            long requestId,
            int winnerId) {

        ElectedMessage msg = ElectedMessage.newBuilder()
                .setRequestId(requestId)
                .setWinnerId(winnerId)
                .build();

        try {
            stub.elected(msg);
        } catch (StatusRuntimeException e) {
            logger.debug("[RPC-FAIL] elected forward failed: {}", e.getStatus());
        }
    }


    public void onElected(long requestId, int winnerId) {

        if (exitInProgress) {
            return;
        }

        EnergyRequest req;
        PlantCoordinationServiceBlockingStub nextStub;
        ElectionContext ctx;
        try {
            stateLock.lock();
            ctx = electionsInProgress.remove(requestId);
            if (ctx == null) {
                return;
            }
            nextStub = ctx.getRingStubs().get(retrieveSuccessorId(this.id,ctx.getRingOrder()));
            req = ctx.getRequest();
            ctx.setParticipant(false);
            electionsCompleted.put(req.getTimestamp(),ctx);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return;
        } finally {
            stateLock.unlock();
        }
        if (plantState.isBusy()) {
            forwardElected(nextStub, requestId, winnerId);
            return;
        }
        if (winnerId == id) {
            handleWin(req);
        } else {
            forwardElected(nextStub, requestId, winnerId);
        }
    }

    private void handleWin(EnergyRequest req) {
    logger.info("[ELECTION-WIN] plant={} request={}", id, req.getTimestamp());

    if (!plantState.tryAcquire(req.getTimestamp())) {
        logger.warn("Plant {} already busy, cannot handle win for request {}", id, req.getTimestamp());
        return;
    }

    produceEnergy(req);
}

    public void start() throws Exception {
        logger.info("Starting ThermalPowerPlant {}", id);

        restClient = new RestClient(REST_SERVER_ADDRESS, REST_SERVER_PORT);

        Plant plant = new Plant(id, portNumber);
        List<Plant> initialNetwork = restClient.registerPlantAndGetNetwork(plant);

        setKnownPlants(initialNetwork);

        startGRPCServer();

        if (!initialNetwork.isEmpty()) {
            announceJoin(initialNetwork);
        }

        buffer = new SlidingWindowBuffer();
        sensor = new PollutionSensor(buffer);
        sensor.start();
        logger.info("Pollution sensor started");

        startHeartbeatThread();

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

            if (exitInProgress) {
                logger.info("[EXIT-COMPLETE] plant {} now idle", id);
                completeExitProcedure();
            }
        }

    }

    public void stop() {
        stopFlag = true;
        logger.info("Stopping ThermalPowerPlant {}", id);

        synchronized (mqttQueue) {
            mqttQueue.notifyAll();
        }


        try {
            if (mqttOutboundThread != null) mqttOutboundThread.interrupt();
            if (heartbeatThread != null) heartbeatThread.interrupt();
        } catch (Exception ignored) {}


        if (sensor != null) {
            sensor.stopMeGently();
        }


        if (grpcServer != null) {
            try {
                grpcServer.shutdownNow();
            } catch (Exception e) {
                logger.warn("Error shutting down gRPC server", e);
            }
        }


        if (mqttClient != null) {
            try {
                if (mqttClient.isConnected()) {
                    mqttClient.disconnect();
                }
                mqttClient.close();
            } catch (MqttException e) {
                logger.error("Error during MQTT shutdown", e);
            }
        }


        try {
            restClient.removePlant(this.id);
        } catch (Exception e) {
            logger.warn("Error removing plant from REST server", e);
        }

        logger.info("ThermalPowerPlant {} stopped", id);

        System.exit(0);
    }

    private void startCommandListener() {
        Thread commandThread = new Thread(() -> {

            while (!stopFlag) {
                try {
                    String cmd = STDIN.nextLine();
                    if ("exit".equalsIgnoreCase(cmd.trim())) {
                        initiateExit();
                        break;
                    }
                } catch (Exception e) {
                    logger.error("Error reading command", e);
                    break;
                }
            }
        });

        commandThread.setDaemon(true);
        commandThread.start();
    }

    public static void main(String[] args) throws Exception {

            int id = insertID();
            ThermalPowerPlant plant = new ThermalPowerPlant(id);

            plant.start();

    }
}
