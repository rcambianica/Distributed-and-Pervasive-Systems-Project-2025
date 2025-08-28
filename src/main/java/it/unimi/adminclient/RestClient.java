package it.unimi.adminclient;

import it.unimi.adminserver.model.Plant;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.Collections;
import java.util.List;

public class RestClient {

    private final HttpClient httpClient;
    private final String serverBaseUrl;
    private final ObjectMapper objectMapper;

    public RestClient(String serverAddress, int serverPort) {
        this.httpClient = HttpClient.newBuilder().version(HttpClient.Version.HTTP_2).build();
        this.serverBaseUrl = "http://" + serverAddress + ":" + serverPort;
        this.objectMapper = new ObjectMapper();
    }

    /**
     * Recupera la lista delle centrali termoelettriche attive dall'Administration Server.
     * Corrisponde a GET /plants nel PlantController.
     * @return Una lista di oggetti Plant, o una lista vuota in caso di errore.
     */
    public List<Plant> getActivePlants() {
        String endpoint = serverBaseUrl + "/plants"; // Endpoint corretto del tuo PlantController
        System.out.println("Invio richiesta a: " + endpoint);
        try {
            HttpRequest request = HttpRequest.newBuilder()
                    .GET()
                    .uri(URI.create(endpoint))
                    .build();

            HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());

            if (response.statusCode() == 200) {
                return objectMapper.readValue(response.body(), new TypeReference<List<Plant>>() {});
            } else {
                System.err.println("Errore nel recupero delle centrali attive. Codice di stato: " + response.statusCode());
                System.err.println("Corpo della risposta: " + response.body());
                return Collections.emptyList();
            }
        } catch (IOException | InterruptedException e) {
            System.err.println("Eccezione durante la richiesta delle centrali attive: " + e.getMessage());
            e.printStackTrace();
            return Collections.emptyList();
        }
    }

    /**
     * Recupera la media delle emissioni di CO2 per un intervallo di tempo specificato.
     * Corrisponde al nuovo GET /pollution/average nel PollutionController.
     * @param t1 Timestamp di inizio (in millisecondi).
     * @param t2 Timestamp di fine (in millisecondi).
     * @return La media delle emissioni di CO2, o null in caso di errore.
     */
    public Double getAverageCO2(long t1, long t2) {
        // Endpoint corretto del tuo PollutionController modificato
        String endpoint = serverBaseUrl + "/pollution/average?t1=" + t1 + "&t2=" + t2;
        System.out.println("Invio richiesta a: " + endpoint);
        try {
            HttpRequest request = HttpRequest.newBuilder()
                    .GET()
                    .uri(URI.create(endpoint))
                    .build();

            HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());

            if (response.statusCode() == 200) {
                return objectMapper.readValue(response.body(), Double.class);
            } else {
                System.err.println("Errore nel recupero delle statistiche CO2. Codice di stato: " + response.statusCode());
                System.err.println("Corpo della risposta: " + response.body());
                return null;
            }
        } catch (IOException | InterruptedException e) {
            System.err.println("Eccezione durante la richiesta delle statistiche CO2: " + e.getMessage());
            e.printStackTrace();
            return null;
        }
    }
}