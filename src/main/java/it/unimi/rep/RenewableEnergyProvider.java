package it.unimi.rep;


import java.util.Random;

import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.eclipse.paho.client.mqttv3.MqttMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class RenewableEnergyProvider {

    private static final String MQTT_BROKER = "tcp://localhost:1883";
    private static final String TOPIC = "energy/requests";
    private static final Logger logger =
            LoggerFactory.getLogger(RenewableEnergyProvider.class);

    private final MqttClient client;
    private final Random rnd = new Random();
    public RenewableEnergyProvider(String clientId) throws MqttException {
        this.client = new MqttClient(MQTT_BROKER, clientId);
        client.connect();
        logger.info("Renewable Energy Provider connected to the MQTT broker");
    }

    public void start() {
        Thread thread = new Thread(() -> {
            while(true) {
                try {
                    EnergyRequest req = generateRequest();
                    String payload = EnergyRequestMapper.serialize(req);
                    client.publish(TOPIC, new MqttMessage(payload.getBytes()));
                    logger.debug("Request published: {}", payload);
                    Thread.sleep(10000);    //10 sec interval
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        });
        thread.start();
    }

    private EnergyRequest generateRequest() {
        int amount = 5000 + rnd.nextInt(10001); // 5000-15000 range
        return new EnergyRequest(amount);
    }

    public static void main(String[] args) throws MqttException {
        RenewableEnergyProvider rep = new RenewableEnergyProvider("REP-1");
        rep.start();
    }
}
