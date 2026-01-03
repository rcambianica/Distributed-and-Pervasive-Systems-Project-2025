package it.unimi.tpp;

import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import it.unimi.grpc.PlantCoordinationServiceGrpc;
import it.unimi.grpc.PlantCoordinationServiceOuterClass.*;

public class PlantCoordinationServiceImpl
        extends PlantCoordinationServiceGrpc.PlantCoordinationServiceImplBase {

    private final int plantId;
    private final ThermalPlantState plantState;

    public PlantCoordinationServiceImpl(int plantId, ThermalPlantState plantState) {
        this.plantId = plantId;
        this.plantState = plantState;
    }

    @Override
    public void checkAvailability(AvailabilityRequest request,
                                  StreamObserver<AvailabilityResponse> responseObserver) {
        PlantStatus status =
                plantState.isBusy()
                        ? PlantStatus.BUSY
                        : PlantStatus.IDLE;
        try {
            AvailabilityResponse response = AvailabilityResponse.newBuilder()
                    .setResponderPlantId(plantId)
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
