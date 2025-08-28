package it.unimi.adminclient;

import it.unimi.adminserver.model.Plant;

import java.time.Instant;
import java.util.List;
import java.util.Scanner;

public class AdminClientApp {

    private static final String SERVER_ADDRESS = "localhost";
    private static final int SERVER_PORT = 8080;

    public static void main(String[] args) {
        Scanner scanner = new Scanner(System.in);
        RestClient restClient = new RestClient(SERVER_ADDRESS, SERVER_PORT);

        System.out.println("--- Client di Amministrazione DESM ---");

        while (true) {
            printMenu();
            String choice = scanner.nextLine();

            try {
                switch (choice) {
                    case "1":
                        listActivePlants(restClient);
                        break;
                    case "2":
                        getPollutionStats(scanner, restClient);
                        break;
                    case "3":
                        System.out.println("Uscita dal client. Arrivederci!");
                        return;
                    default:
                        System.out.println("Scelta non valida. Inserire un numero tra 1 e 3.");
                        break;
                }
            } catch (Exception e) {
                System.err.println("Si è verificato un errore inaspettato: " + e.getMessage());
                e.printStackTrace();
            }
            System.out.println("\nPremi INVIO per continuare...");
            scanner.nextLine();
        }
    }

    private static void printMenu() {
        System.out.println("\n--- Menu ---");
        System.out.println("1. Visualizza centrali termoelettriche attive");
        System.out.println("2. Calcola media emissioni CO2 per intervallo di tempo");
        System.out.println("3. Esci");
        System.out.print("Inserisci la tua scelta: ");
    }

    /**
     * Richiede e visualizza la lista delle centrali attive.
     * @param restClient L'istanza di RestClient per comunicare con il server.
     */
    private static void listActivePlants(RestClient restClient) {
        System.out.println("\n--- Centrali Attive ---");
        List<Plant> plants = restClient.getActivePlants();
        if (plants != null && !plants.isEmpty()) {
            plants.forEach(System.out::println);
        } else {
            System.out.println("Nessuna centrale attiva trovata.");
        }
    }

    /**
     * Richiede all'utente i timestamp e poi recupera e visualizza le statistiche sulle emissioni di CO2.
     * @param scanner L'istanza di Scanner per leggere l'input dell'utente.
     * @param restClient L'istanza di RestClient per comunicare con il server.
     */
    private static void getPollutionStats(Scanner scanner, RestClient restClient) {
        System.out.println("\n--- Statistiche Emissioni CO2 ---");
        long t1, t2;
        try {
            System.out.print("Vuoi inserire i millisecondi manualmente? (s/n): ");
            String risposta = scanner.nextLine();
            if (risposta.equalsIgnoreCase("n")) {
                t1 = Instant.now().minusSeconds(3600).toEpochMilli(); // ultima ora
                t2 = Instant.now().toEpochMilli();
            } else {
                System.out.print("Inserisci il timestamp di inizio (t1 in millisecondi): ");
                t1 = Long.parseLong(scanner.nextLine());

                System.out.print("Inserisci il timestamp di fine (t2 in millisecondi): ");
                t2 = Long.parseLong(scanner.nextLine());

                if (t1 < 0 || t2 < 0 || t1 > t2) {
                    System.out.println("Errore: I timestamp devono essere positivi e t1 non può essere maggiore di t2.");
                    return;
                }
            }

            // parte comune: chiamata al server
            Double avgCO2 = restClient.getAverageCO2(t1, t2);
            if (avgCO2 != null) {
                System.out.printf("La media delle emissioni di CO2 tra %d e %d è: %.2f g%n", t1, t2, avgCO2);
            } else {
                System.out.println("Impossibile recuperare le statistiche CO2 o errore dal server.");
            }
        } catch (NumberFormatException e) {
            System.err.println("Input non valido. Inserire un numero valido per il timestamp.");
        } catch (Exception e) {
            System.err.println("Errore durante l'ottenimento delle statistiche CO2: " + e.getMessage());
            e.printStackTrace();
        }
    }
}
