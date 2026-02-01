package it.unimi.tpp;

import it.unimi.rep.EnergyRequest;

import java.util.List;

public class ElectionContext {

    private final EnergyRequest request;
    private final float myBid;
    private final List<Integer> electionView; // SNAPSHOT IMMUTABILE
    private boolean participant;

    public ElectionContext(EnergyRequest request,
                           float myBid,
                           List<Integer> electionView) {
        this.request = request;
        this.myBid = myBid;
        this.electionView = electionView;
        this.participant = false;
    }

    public EnergyRequest getRequest() {
        return request;
    }

    public float getMyBid() {
        return myBid;
    }

    public List<Integer> getElectionView() {
        return electionView;
    }

    public boolean isParticipant() {
        return participant;
    }

    public void setParticipant(boolean participant) {
        this.participant = participant;
    }
}
