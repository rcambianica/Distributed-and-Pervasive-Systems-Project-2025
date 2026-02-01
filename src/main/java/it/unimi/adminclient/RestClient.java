package it.unimi.adminclient;

import it.unimi.adminserver.model.Plant;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.fasterxml.jackson.databind.SerializationFeature;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.List;

import static it.unimi.tpp.ThermalPowerPlant.REST_SERVER_ADDRESS;
import static it.unimi.tpp.ThermalPowerPlant.REST_SERVER_PORT;

public class RestClient {

    private static final Logger logger =
            LoggerFactory.getLogger(RestClient.class);

    private final HttpClient httpClient;
    private final String serverBaseUrl;
    private final ObjectMapper objectMapper;

    public RestClient(String serverAddress, int serverPort) {
        this.httpClient = HttpClient.newHttpClient();
        this.serverBaseUrl = "http://" + serverAddress + ":" + serverPort;
        this.objectMapper = new ObjectMapper()
                .registerModule(new JavaTimeModule())
                .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
    }

    public Double getAverageCO2(long t1, long t2) {
        String endpoint = serverBaseUrl + "/pollution/average?t1=" + t1 + "&t2=" + t2;
        logger.debug("GET {}", endpoint);

        try {
            HttpRequest request = HttpRequest.newBuilder()
                    .uri(URI.create(endpoint))
                    .GET()
                    .build();

            HttpResponse<String> response =
                    httpClient.send(request, HttpResponse.BodyHandlers.ofString());

            if (response.statusCode() == 200) {
                return objectMapper.readValue(response.body(), Double.class);
            }

            logger.warn("GET /pollution/average returned status {}", response.statusCode());
            return null;

        } catch (IOException | InterruptedException e) {
            logger.error("Error during GET /pollution/average", e);
            return null;
        }
    }

    public List<Plant> registerPlantAndGetNetwork(Plant plant) {
        String endpoint = serverBaseUrl + "/plants";
        try {
            String body = objectMapper.writeValueAsString(plant);

            HttpRequest request = HttpRequest.newBuilder()
                    .uri(URI.create(endpoint))
                    .header("Content-Type", "application/json")
                    .POST(HttpRequest.BodyPublishers.ofString(body))
                    .build();

            HttpResponse<String> response =
                    httpClient.send(request, HttpResponse.BodyHandlers.ofString());

            int status = response.statusCode();
            if (status >= 200 && status < 300) {
                logger.info("Plant {} successfully registered (status {})",
                        plant.getId(), status);
            } else {
                logger.warn("Unexpected response {} during plant registration", status);
            }


        } catch (Exception e) {
            logger.error("Error during plant registration", e);
        }

        return getNetwork();
    }


    public List<Plant> getNetwork() {
        String endpoint = serverBaseUrl + "/plants";
        try {
            HttpRequest request = HttpRequest.newBuilder()
                    .uri(URI.create(endpoint))
                    .GET()
                    .build();

            HttpResponse<String> response =
                    httpClient.send(request, HttpResponse.BodyHandlers.ofString());

            if (response.statusCode() == 200) {
                return objectMapper.readValue(
                        response.body(),
                        new TypeReference<>() {
                        }
                );
            }

            logger.warn("GET /plants returned status {}", response.statusCode());
            return null;

        } catch (Exception e) {
            logger.error("Error during plant network retrieval", e);
            return null;
        }
    }

    public void removePlant(int id) {
        String endpoint = serverBaseUrl + "/plants/" + id;
        try {
            HttpRequest request = HttpRequest.newBuilder()
                    .uri(URI.create(endpoint))
                    .DELETE()
                    .build();
            httpClient.send(request, HttpResponse.BodyHandlers.discarding());
            logger.info("Plant {} removed successfully", id);
        } catch (Exception e) {
            logger.error("Error removing plant {}", id, e);
        }
    }

    public void sendHeartbeat(int id) {
        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(
                        "http://" + REST_SERVER_ADDRESS + ":" + REST_SERVER_PORT +
                                "/plants/" + id + "/heartbeat"
                ))
                .POST(HttpRequest.BodyPublishers.noBody())
                .build();

        try {
            httpClient.send(request, HttpResponse.BodyHandlers.discarding());
        } catch (Exception e) {
            logger.error("Error sending heartbeat", e);
        }
    }

}