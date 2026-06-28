package com.myorg.Service;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import java.util.UUID;

@Service
public class LogDlqService {

    private final KafkaTemplate<String, Object> kafkaTemplate;
    private static final Logger log = LoggerFactory.getLogger(LogDlqService.class);
    private static final String DLQ_TOPIC = "app-logs-dlq";

    public LogDlqService(KafkaTemplate<String, Object> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    public void send(Object payload, String reason) {
        log.warn("Routing to DLQ. Reason: {}", reason);
        kafkaTemplate.send(DLQ_TOPIC, UUID.randomUUID().toString(), payload);
    }
}