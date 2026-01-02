package it.unimi.tpp;

import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import it.unimi.grpc.PlantCoordinationServiceGrpc;
import it.unimi.grpc.PlantCoordinationServiceOuterClass.*;

public class PlantCoordinationServiceImpl
        extends PlantCoordinationServiceGrpc.PlantCoordinationServiceImplBase {

    @Override
    public void sendIdAndPort(TPPIdentity sender, StreamObserver<Ack> responseObserver) {

        Ack reply = Ack.newBuilder()
                .setContent("Presentation received")
                .build();

        responseObserver.onNext(reply);
        responseObserver.onCompleted();
    }

    @Override
    public void checkAvailability(AvailabilityRequest request,
                                  StreamObserver<AvailabilityResponse> responseObserver) {
        try {
            // da sistemare
            int localPlantId = 0;
            PlantStatus status = PlantStatus.IDLE;

            AvailabilityResponse response = AvailabilityResponse.newBuilder()
                    .setResponderPlantId(localPlantId)
                    .setRequestId(request.getRequestId())
                    .setStatus(status)
                    .build();

            responseObserver.onNext(response);
            responseObserver.onCompleted();
        } catch (Exception e) {
            responseObserver.onError(
                    Status.INTERNAL
                            .withDescription("Error in checkAvailability")
                            .withCause(e)
                            .asRuntimeException()
            );
        }
    }
}
