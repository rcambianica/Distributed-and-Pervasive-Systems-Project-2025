package it.unimi.adminserver.model;

public class Plant {
    private String id;
    private String address;
    private int port;

    public Plant() {}

    public Plant(String id, String address, int port) {
        this.id = id;
        this.address = address;
        this.port = port;
    }

    public String getId() { return id; }
    public String getAddress() { return address; }
    public int getPort() { return port; }

    public void setId(String id) { this.id = id; }
    public void setAddress(String address) { this.address = address; }
    public void setPort(int port) { this.port = port; }
}
