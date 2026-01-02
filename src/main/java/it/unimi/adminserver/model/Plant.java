package it.unimi.adminserver.model;

public class Plant {
    private int id;
    private static final String address = "localhost";
    private int port;

    public Plant() {};
    public Plant(int id, int port) {
        setId(id);
        setPort(port);
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

    @Override
    public String toString() {
        return "Plant{" +
                "id='" + id + '\'' +
                ", port=" + port +
                '}';
    }
}
