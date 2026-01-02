package it.unimi.adminclient;

import it.unimi.adminserver.model.Plant;
import it.unimi.adminserver.model.PollutionRecord;

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
import java.util.Collections;
import java.util.List;

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

    public List<Plant> getActivePlants() {
        String endpoint = serverBaseUrl + "/plants";
        logger.debug("GET {}", endpoint);

        try {
            HttpRequest request = HttpRequest.newBuilder()
                    .uri(URI.create(endpoint))
                    .GET()
                    .build();

            HttpResponse<String> response =
                    httpClient.send(request, HttpResponse.BodyHandlers.ofString());

            if (response.statusCode() == 200) {
                return objectMapper.readValue(response.body(), new TypeReference<>() {});
            }

            logger.warn("GET /plants returned status {}", response.statusCode());
            return Collections.emptyList();

        } catch (IOException | InterruptedException e) {
            logger.error("Error during GET /plants", e);
            return Collections.emptyList();
        }
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

    public List<Plant> addPlantAndGetNetwork(Plant plant) {
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

            if (response.statusCode() == 200) {
                logger.info("Plant {} registered successfully", plant.getId());
                return objectMapper.readValue(response.body(), new TypeReference<>() {});
            }

            logger.warn("POST /plants returned status {}", response.statusCode());
            return Collections.emptyList();

        } catch (Exception e) {
            logger.error("Error during plant registration", e);
            return Collections.emptyList();
        }
    }

    public void addPollutionRecord(PollutionRecord record) {
        String endpoint = serverBaseUrl + "/pollution";

        try {
            String body = objectMapper.writeValueAsString(record);

            HttpRequest request = HttpRequest.newBuilder()
                    .uri(URI.create(endpoint))
                    .header("Content-Type", "application/json")
                    .POST(HttpRequest.BodyPublishers.ofString(body))
                    .build();

            httpClient.send(request, HttpResponse.BodyHandlers.discarding());
            logger.debug("Pollution record sent for plant {}", record.getPlantId());

        } catch (Exception e) {
            logger.error("Error sending pollution record", e);
        }
    }
}