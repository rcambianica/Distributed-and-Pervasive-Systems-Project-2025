package it.unimi.tpp;

public class ElectionContext {
    private final long requestId;
    private volatile boolean participant;
    private final float myBid;
    private float bestBid;
    private int bestCandidateId;
    private volatile boolean completed;
    public ElectionContext(long requestId, float myBid) {
        this.requestId = requestId;
        this.participant = false;
        this.myBid = myBid;
        this.bestBid = -1;
        this.bestCandidateId = -1;
        this.completed = false;
    }

    public long getRequestId() {
        return requestId;
    }

    public boolean isParticipant() {
        return participant;
    }

    public void setParticipant(boolean participant) {
        this.participant = participant;
    }

    public float getMyBid() {
        return myBid;
    }

    public float getBestBid() {
        return bestBid;
    }

    public void setBestBid(float bestBid) {
        this.bestBid = bestBid;
    }

    public int getBestCandidateId() {
        return bestCandidateId;
    }

    public void setBestCandidateId(int bestCandidateId) {
        this.bestCandidateId = bestCandidateId;
    }

    public boolean isCompleted() {
        return completed;
    }

    public void setCompleted(boolean completed) {
        this.completed = completed;
    }
}