package it.unimi.tpp;

import io.grpc.stub.StreamObserver;
import it.unimi.grpc.PlantCoordinationServiceGrpc;
import it.unimi.grpc.PlantCoordinationServiceOuterClass.ElectionMessage;
import it.unimi.grpc.PlantCoordinationServiceOuterClass.ElectedMessage;
import it.unimi.grpc.PlantCoordinationServiceOuterClass.Empty;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Implementazione gRPC minimale.
 * Non contiene logica di elezione.
 * Delegazione totale al ThermalPowerPlant.
 */
public class PlantCoordinationServiceImpl
        extends PlantCoordinationServiceGrpc.PlantCoordinationServiceImplBase {

    private static final Logger logger =
            LoggerFactory.getLogger(PlantCoordinationServiceImpl.class);

    private final ThermalPowerPlant plant;

    public PlantCoordinationServiceImpl(ThermalPowerPlant plant) {
        this.plant = plant;
    }

    /**
     * Ricezione messaggio <ELECTION, requestId, candidateId, bid>
     * Forward-only: la decisione è interamente locale al plant.
     */
    @Override
    public void election(ElectionMessage request,
                         StreamObserver<Empty> responseObserver) {

        logger.debug(
                "[RPC] ELECTION received: request={} cand={} bid={}",
                request.getRequestId(),
                request.getCandidateId(),
                request.getBid()
        );

        plant.onElectionMessage(
                request.getRequestId(),
                request.getCandidateId(),
                request.getBid()
        );

        responseObserver.onNext(Empty.getDefaultInstance());
        responseObserver.onCompleted();
    }

    /**
     * Ricezione messaggio <ELECTED, requestId, winnerId>
     * Serve solo a propagare il vincitore e terminare l’elezione.
     */
    @Override
    public void elected(ElectedMessage request,
                        StreamObserver<Empty> responseObserver) {

        logger.debug(
                "[RPC] ELECTED received: request={} winner={}",
                request.getRequestId(),
                request.getWinnerId()
        );

        plant.onElected(
                request.getRequestId(),
                request.getWinnerId()
        );

        responseObserver.onNext(Empty.getDefaultInstance());
        responseObserver.onCompleted();
    }

}

