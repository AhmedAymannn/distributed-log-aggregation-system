package com.myorg.Service;


import ch.qos.logback.classic.Logger;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.myorg.common.LogEvent;
import com.myorg.document.FailedLog;
import com.myorg.document.LogDocument;
import com.myorg.repository.FailedLogRepository;
import org.springframework.data.mongodb.BulkOperationException;
import org.springframework.stereotype.Service;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.BulkOperations;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;
import com.mongodb.bulk.BulkWriteError;
import java.util.*;
import java.util.stream.Collectors;


@Service
public class AggregatorService {

    private final MongoTemplate mongoTemplate;
    private final ObjectMapper objectMapper;
    private final FailedLogRepository failedLogRepository;

    public AggregatorService(MongoTemplate mongoTemplate,
                             ObjectMapper objectMapper,
                             FailedLogRepository failedLogRepository) {
        this.mongoTemplate = mongoTemplate;
        this.objectMapper = objectMapper;
        this.failedLogRepository = failedLogRepository;
    }

    public void processBatch(List<LogEvent> events) {

        if (events == null || events.isEmpty()) return;

        List<LogDocument> validDocs = new ArrayList<>();
        BulkOperations bulkOps =
                mongoTemplate.bulkOps(BulkOperations.BulkMode.UNORDERED, LogDocument.class);

        for (LogEvent event : events) {
            try {
                LogDocument doc = objectMapper.convertValue(event, LogDocument.class);
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
// 3. EXECUTION & ERROR RECONCILIATION
        try {
            bulkOps.execute();
        } catch (BulkOperationException ex) {
            // Handle partial failures from MongoDB (e.g., uniqueness constraint)
            ex.getErrors().forEach(error -> {
                LogDocument failedDoc = validDocs.get(error.getIndex());
                // Attempt a final single save or direct to DLQ
                handleMongoFailure(failedDoc, error.getMessage());
            });
        } catch (Exception e) {
            // If DB is down, rethrow to trigger Kafka retry
            log.error("Fatal MongoDB error: {}", e.getMessage());
            throw e;
        }
    }




    private boolean isValid(LogDocument doc) {
        return doc.getId() != null &&
                doc.getTimestamp() != null &&
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



