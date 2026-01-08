package it.unimi.tpp;


import io.grpc.stub.StreamObserver;
import it.unimi.grpc.PlantCoordinationServiceGrpc;
import it.unimi.grpc.PlantCoordinationServiceOuterClass.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PlantCoordinationServiceImpl
        extends PlantCoordinationServiceGrpc.PlantCoordinationServiceImplBase {

    private static final Logger logger =
            LoggerFactory.getLogger(PlantCoordinationServiceImpl.class);

    private final ThermalPowerPlant tpp;

    public PlantCoordinationServiceImpl(ThermalPowerPlant tpp) {
        this.tpp = tpp;
    }

    /**
     * Ricezione messaggio di election (Chang & Roberts).
     * La reazione NON è una risposta RPC, ma una decisione locale
     * che può portare all'invio di un nuovo messaggio verso il successore.
     */
    @Override
    public void election(ElectionMessage request,
                         StreamObserver<Empty> responseObserver) {

        logger.debug(
                "[RPC-ELECTION-RECEIVED] plant={} request={} cand={} bid={}",
                tpp.getId(),
                request.getRequestId(),
                request.getCandidateId(),
                request.getBid()
        );

        tpp.onElectionMessage(request);

        responseObserver.onNext(Empty.getDefaultInstance());
        responseObserver.onCompleted();
    }

    @Override
    public void leaderElected(WinnerMessage request,
                              StreamObserver<Empty> responseObserver) {

        tpp.onLeaderElected(request.getRequestId(), request.getWinnerId());

        responseObserver.onNext(Empty.getDefaultInstance());
        responseObserver.onCompleted();
    }

    /**
     * Notifica di uscita di un altro plant dalla rete.
     * Aggiornamento immediato della vista di rete locale.
     */
    @Override
    public void notifyExit(ExitNotification request,
                           StreamObserver<Empty> responseObserver) {

        logger.info(
                "[RPC-EXIT-RECEIVED] plant={} exited",
                request.getPlantId()
        );

        tpp.handleRemoteExit(request.getPlantId());

        responseObserver.onNext(Empty.getDefaultInstance());
        responseObserver.onCompleted();
    }
}
