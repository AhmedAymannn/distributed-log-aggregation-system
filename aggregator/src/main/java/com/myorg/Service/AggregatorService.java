package com.myorg.Service;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.myorg.common.LogEvent;
import com.myorg.document.LogDocument;
import org.springframework.data.mongodb.BulkOperationException;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.BulkOperations;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;


@Service
public class AggregatorService {

    private final MongoTemplate mongoTemplate;
    private final ObjectMapper objectMapper;
    private final KafkaTemplate<String, Object> kafkaTemplate;
    private static final Logger log = LoggerFactory.getLogger(AggregatorService.class);

    public AggregatorService(MongoTemplate mongoTemplate,
                             ObjectMapper objectMapper,
                             KafkaTemplate<String, Object> kafkaTemplate) {

        this.mongoTemplate = mongoTemplate;
        this.objectMapper = objectMapper;
        this.kafkaTemplate = kafkaTemplate ;
    }

    public void processBatch(List<LogEvent> events) {

        if (events == null || events.isEmpty()) return;

        List<LogDocument> validDocs = new ArrayList<>();
        BulkOperations bulkOps =
                mongoTemplate.bulkOps(BulkOperations.BulkMode.UNORDERED, LogDocument.class);

        for (LogEvent event : events) {
            try {
                LogDocument doc = objectMapper.convertValue(event, LogDocument.class);
                if (doc.getId() == null) {
                    doc.setId(generateId(event));
                }

                if (isValid(doc)) {
                    validDocs.add(doc);
                } else {
                    sendToDlq(event, "VALIDATION_FAILED");
                }
            } catch (Exception e) {
                sendToDlq(event, "MAPPING_FAILED");
            }
        }

        if (validDocs.isEmpty()) return;

        for (LogDocument doc : validDocs) {
            Query query = new Query(Criteria.where("id").is(doc.getId()));

            Update update = new Update()
                    .set("timestamp", doc.getTimestamp())
                    .set("serviceName", doc.getServiceName())
                    .set("logLevel", doc.getLogLevel())
                    .set("traceId", doc.getTraceId())
                    .set("message", doc.getMessage())
                    .set("durationMs", doc.getDurationMs())
                    .set("metadata", doc.getMetadata());

            bulkOps.upsert(query, update);
        }

        try {
            bulkOps.execute();
        } catch (BulkOperationException ex) {
            // Use a copy of validDocs mapped by id for safe error reconciliation
            Map<String, LogDocument> docById = new HashMap<>();
            validDocs.forEach(d -> docById.put(d.getId(), d));

            ex.getErrors().forEach(error -> {
                // Extract the id from the error message since index is unreliable
                // in unordered bulk ops — instead log and DLQ by iterating errors
                log.error("Bulk write error at index {}: {}", error.getIndex(), error.getMessage());
                try {
                    LogDocument failedDoc = validDocs.get(error.getIndex());
                    handleMongoFailure(failedDoc, error.getMessage());
                } catch (IndexOutOfBoundsException e) {
                    log.error("Could not reconcile bulk error at index {}", error.getIndex());
                }
            });
        } catch (Exception e) {
            log.error("Fatal MongoDB error: {}", e.getMessage());
            throw e;
        }
    }

    // Deterministic id — same log content always produces same id
    private String generateId(LogEvent event) {
        String raw = event.serviceName()
                + event.timestamp()
                + event.traceId();
        return UUID.nameUUIDFromBytes(raw.getBytes()).toString();
    }

    private boolean isValid(LogDocument doc) {
        return doc.getTimestamp() != null &&
                doc.getServiceName() != null;
    }
    private void handleMongoFailure(LogDocument doc, String reason) {
        try {
            mongoTemplate.save(doc); // Final effort retry
        } catch (Exception e) {
            sendToDlq(doc, "MONGO_FINAL_FAILURE: " + reason);
        }
    }

    private void sendToDlq(Object payload, String reason) {
        log.warn("Routing to DLQ. Reason: {}", reason);
        kafkaTemplate.send("app-logs-dlq", UUID.randomUUID().toString(), payload);
    }
}



