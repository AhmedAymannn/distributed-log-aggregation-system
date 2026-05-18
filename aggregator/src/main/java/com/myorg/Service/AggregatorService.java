package com.myorg.Service;

import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.stereotype.Service;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;

@Service
public class AggregatorService {

    // Define the path to your test output file
    private static final String OUTPUT_FILE_PATH = "received-app-logs.txt";

    @KafkaListener(topics = "app-logs", groupId = "aggregator-group")
    public void processLogs(String rawLog,
                            @Header(KafkaHeaders.RECEIVED_PARTITION) int partition,
                            @Header(KafkaHeaders.OFFSET) long offset) {

        // 1. Still print to console so you can see it instantly
        System.out.println("-> Consumer intercepted log from partition " + partition + ". Writing to file...");

        // 2. Format the message with metadata context so it looks structured in your file
        String formattedLogEntry = String.format(
                "[KAFKA-METADATA | Partition: %d | Offset: %d]%n%s%n",
                partition, offset, rawLog
        );

        // 3. Throw it to the file
        saveToFile(formattedLogEntry);
    }

    private synchronized void saveToFile(String content) {
        // Using standard Java FileWriter in append mode (true)
        try (BufferedWriter writer = new BufferedWriter(new FileWriter(OUTPUT_FILE_PATH, true))) {
            writer.write(content);
            writer.newLine(); // Add an extra space between log events
        } catch (IOException e) {
            System.err.println("Error writing consumed log to file: " + e.getMessage());
        }
    }
}