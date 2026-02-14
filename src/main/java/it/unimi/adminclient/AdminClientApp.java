package it.unimi.adminclient;

import it.unimi.adminserver.model.Plant;

import java.time.Instant;
import java.util.List;
import java.util.Scanner;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AdminClientApp {

    private static final String SERVER_ADDRESS = "localhost";
    private static final int SERVER_PORT = 8080;

    private static final Logger logger =
            LoggerFactory.getLogger(AdminClientApp.class);

    public static void main(String[] args) {
        Scanner scanner = new Scanner(System.in);
        RestClient restClient = new RestClient(SERVER_ADDRESS, SERVER_PORT);

        logger.info("Admin Client connected to the Administration Server");

        // Runs menu loop; handles user input; logs errors
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
                        logger.info("Admin Client terminated by user");
                        return;
                    default:
                        logger.debug("Invalid menu choice: {}", choice);
                        break;
                }
            } catch (Exception e) {
                logger.error("Unexpected error in AdminClientApp main loop", e);
            }

            System.out.println("\nPress enter to continue...");
            scanner.nextLine();
        }
    }

    private static void printMenu() {
        System.out.println("\n--- Menu ---");
        System.out.println("1. Visualize active Thermal Power Plants");
        System.out.println("2. Calculate average CO2 emissions for a specific time interval");
        System.out.println("3. Exit");
        System.out.print("Insert your choice: ");
    }

    /**
     * Retrieves and prints active plants; logs empty result
     */
    private static void listActivePlants(RestClient restClient) {
        System.out.println("\n--- Active Thermal Power Plants ---");
        List<Plant> plants = restClient.getNetwork();

        if (plants != null && !plants.isEmpty()) {
            plants.forEach(System.out::println);
        } else {
            System.out.println("No active plants found. Try again later.");
            logger.debug("No active plants returned by server");
        }
    }

    private static void getPollutionStats(Scanner scanner, RestClient restClient) {
        System.out.println("\n--- CO2 emissions statistics ---");

        long t1;
        long t2;

        // Retrieves and prints average CO2 emissions for given interval
        try {
            System.out.print("Want to insert milliseconds manually? (s/n): ");
            String risp = scanner.nextLine();

            // Gets timestamps from input or defaults; validates input
            if (risp.equalsIgnoreCase("n")) {
                t1 = Instant.now().minusSeconds(3600).toEpochMilli();
                t2 = Instant.now().toEpochMilli();
                logger.debug("Using default time interval: last hour");
            } else {
                System.out.print("Insert the start timestamp (t1 in milliseconds): ");
                t1 = Long.parseLong(scanner.nextLine());

                System.out.print("Insert the end timestamp (t2 in milliseconds): ");
                t2 = Long.parseLong(scanner.nextLine());

                if (t1 < 0 || t2 < 0 || t1 > t2) {
                    System.out.println("Error: timestamps must be positive and t1 cannot be bigger than t2.");
                    logger.debug("Invalid timestamps provided: t1={}, t2={}", t1, t2);
                    return;
                }
            }

            Double avgCO2 = restClient.getAverageCO2(t1, t2);

            if (avgCO2 != null) {
                System.out.printf(
                        "The average CO2 emission between %d and %d is: %.2f g%n",
                        t1, t2, avgCO2
                );
                logger.info("CO2 statistics retrieved for interval {} - {}", t1, t2);
            } else {
                System.out.println("Impossible to retrieve CO2 stats or server error.");
                logger.warn("Server returned null CO2 statistics for interval {} - {}", t1, t2);
            }

        } catch (NumberFormatException e) {
            System.err.println("Invalid input. Insert a valid timestamp number.");
            logger.debug("Invalid numeric input for timestamps", e);
        } catch (Exception e) {
            logger.error("Error during CO2 stats retrieval", e);
        }
    }
}

