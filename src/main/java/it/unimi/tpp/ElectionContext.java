package it.unimi.tpp;

import it.unimi.grpc.PlantCoordinationServiceGrpc;
import it.unimi.rep.EnergyRequest;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ElectionContext {

    private final List<Integer> ringOrder;
    private final Map<Integer, PlantCoordinationServiceGrpc.PlantCoordinationServiceBlockingStub> ringStubs;
    private final EnergyRequest request;
    private final float myBid;
    private volatile boolean participant;

    public ElectionContext(List<Integer> ringOrder, Map<Integer, PlantCoordinationServiceGrpc.PlantCoordinationServiceBlockingStub> ringStubs, EnergyRequest request, float myBid, boolean participant) {
        this.ringOrder = Collections.unmodifiableList(ringOrder);
        this.ringStubs = new HashMap<>(ringStubs);
        this.request = request;
        this.myBid = myBid;
        this.participant = participant;
    }

    public Map<Integer, PlantCoordinationServiceGrpc.PlantCoordinationServiceBlockingStub> getRingStubs() {
        return ringStubs;
    }

    public List<Integer> getRingOrder() {
        return ringOrder;
    }

    public EnergyRequest getRequest() {
        return request;
    }

    public float getMyBid() {
        return myBid;
    }

    public boolean isParticipant() {
        return participant;
    }

    public void setParticipant(boolean participant) {
        this.participant = participant;
    }
}
