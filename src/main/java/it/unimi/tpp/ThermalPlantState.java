package it.unimi.tpp;

public class ThermalPlantState {

    public enum Status {
        IDLE,
        BUSY
    }

    private Status status;
    private Long currentRequestId;

    public ThermalPlantState() {
        this.status = Status.IDLE;
        this.currentRequestId = null;
    }

    public synchronized boolean tryAcquire(Long requestId) {
        if (status == Status.IDLE) {
            status = Status.BUSY;
            currentRequestId = requestId;
            return true;
        }
        return false;
    }

    public synchronized void release(Long requestId) {
        if (status == Status.BUSY && currentRequestId != null
                && currentRequestId.equals(requestId)) {
            status = Status.IDLE;
            currentRequestId = null;
        }
    }

    public synchronized boolean isBusy() {
        return status == Status.BUSY;
    }
}
