package it.unimi.tpp;

import io.grpc.stub.StreamObserver;
import it.unimi.grpc.PlantCoordinationServiceGrpc;
import it.unimi.grpc.PlantCoordinationServiceOuterClass.*;
import it.unimi.grpc.PlantCoordinationServiceOuterClass.ElectionMessage;
import it.unimi.grpc.PlantCoordinationServiceOuterClass.ElectedMessage;
import it.unimi.grpc.PlantCoordinationServiceOuterClass.Empty;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PlantCoordinationServiceImpl
        extends PlantCoordinationServiceGrpc.PlantCoordinationServiceImplBase {

    private static final Logger logger = LoggerFactory.getLogger(PlantCoordinationServiceImpl.class);

    private final ThermalPowerPlant plant;

    public PlantCoordinationServiceImpl(ThermalPowerPlant plant) {
        this.plant = plant;
    }

    @Override
    public void election(ElectionMessage request, StreamObserver<Empty> responseObserver) {

        logger.debug("[RPC] ELECTION received: request={} cand={} bid={}", request.getRequestId(), request.getCandidateId(), request.getBid());

        plant.onElectionMessage(request.getRequestId(), request.getCandidateId(), request.getBid());

        responseObserver.onNext(Empty.getDefaultInstance());
        responseObserver.onCompleted();
    }

    @Override
    public void elected(ElectedMessage request, StreamObserver<Empty> responseObserver) {

        logger.debug("[RPC] ELECTED received: request={} winner={}", request.getRequestId(), request.getWinnerId());

        plant.onElected(request.getRequestId(), request.getWinnerId());

        responseObserver.onNext(Empty.getDefaultInstance());
        responseObserver.onCompleted();
    }

    @Override
    public void notifyJoin(JoinNotification request,
                           StreamObserver<JoinAck> responseObserver) {

        int pid = request.getPlantId();
        int port = request.getPort();

        logger.info("[RPC] JOIN received from plant {}", pid);

        plant.handleRemoteJoin(pid, port);

        JoinAck ack = JoinAck.newBuilder()
                .setPlantId(plant.getId())
                .setPort(plant.getPortNumber())
                .build();

        responseObserver.onNext(ack);
        responseObserver.onCompleted();
    }

    @Override
    public void notifyExit(ExitNotification request,
                           StreamObserver<Empty> responseObserver) {

        int exitingPlantId = request.getPlantId();
        logger.info("[RPC] EXIT received from plant {}", exitingPlantId);
        plant.handleRemoteExit(exitingPlantId);
        responseObserver.onNext(Empty.getDefaultInstance());
        responseObserver.onCompleted();
    }
}

