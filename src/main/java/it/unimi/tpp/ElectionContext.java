package it.unimi.tpp;

import it.unimi.rep.EnergyRequest;

public class ElectionContext {

    private final EnergyRequest request;
    private final float myBid;
    private boolean participant;

    public ElectionContext(EnergyRequest request) {
        this.request = request;
        this.myBid = -1;
        this.participant = false;
    }

    public ElectionContext(EnergyRequest request, float myBid) {
        this.request = request;
        this.myBid = myBid;
        this.participant = false;
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

