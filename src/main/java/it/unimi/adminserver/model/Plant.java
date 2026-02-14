package it.unimi.adminserver.model;

public class Plant {
    private int id;
    private static final String address = "localhost";
    private int port;
    private volatile long lastHeartbeat;

    public Plant() {}

    public Plant(int id, int port) {
        setId(id);
        setPort(port);
        this.lastHeartbeat = System.currentTimeMillis();
    }

    public int getId() { return id; }
    public int getPort() { return port; }


    public void setId(int id) {
        this.id = id;
    }

    public void setPort(int port) {
        if (port < 1 || port > 65535) {
            throw new IllegalArgumentException("port must be between 1 and 65535");
        }
        this.port = port;
    }

    public void heartbeat() {
        this.lastHeartbeat = System.currentTimeMillis();
    }

    public long getLastHeartbeat() {
        return lastHeartbeat;
    }

    @Override
    public String toString() {
        return "Plant{" +
                "id=" + id +
                ", address='" + address + '\'' +
                ", port=" + port +
                ", lastHeartbeat=" + lastHeartbeat +
                '}';
    }
}
