package it.unimi.tpp;

public class ElectionContext {

    private final long requestId;
    private final float myBid;
    private boolean participant;

    public ElectionContext(long requestId, float myBid) {
        this.requestId = requestId;
        this.myBid = myBid;
        this.participant = false;
    }

    public long getRequestId() {
        return requestId;
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