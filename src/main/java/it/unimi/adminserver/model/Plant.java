package it.unimi.adminserver.model;

public class Plant {
    private String id;
    private static final String address = "localhost";
    private int port;

    public Plant() {}

    public Plant(String id, int port) {
        setId(id);
        setPort(port);
    }

    public String getId() { return id; }
    public int getPort() { return port; }

    public void setId(String id) {
        if (id == null || id.trim().isEmpty()) {
            throw new IllegalArgumentException("id must be non-null and non-empty");
        }
        this.id = id;
    }

    public void setPort(int port) {
        if (port < 1 || port > 65535) {
            throw new IllegalArgumentException("port must be between 1 and 65535");
        }
        this.port = port;
    }

    @Override
    public String toString() {
        return "Plant{" +
                "id='" + id + '\'' +
                ", port=" + port +
                '}';
    }
}
