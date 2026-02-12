package it.unimi.tpp;

import it.unimi.rep.EnergyRequest;

import java.util.Collections;
import java.util.List;

public class ElectionContext {

    private final List<Integer> electionView;
    private final EnergyRequest request;
    private final float myBid;
    private boolean participant;
    private volatile boolean completed = false;

    public ElectionContext(List<Integer> electionView, EnergyRequest request, float myBid, boolean participant) {
        this.electionView = Collections.unmodifiableList(electionView);
        this.request = request;
        this.myBid = myBid;
        this.participant = participant;
    }

    public List<Integer> getElectionView() {
        return electionView;
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

    public boolean isCompleted() {
        return completed;
    }

    public void setCompleted(boolean completed) {
        this.completed = completed;
    }
}
