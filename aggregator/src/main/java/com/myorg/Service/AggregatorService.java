package com.myorg.Service;

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

        BulkOperations bulkOps =
                mongoTemplate.bulkOps(BulkOperations.BulkMode.UNORDERED, LogDocument.class);

        List<LogDocument> validDocs = new ArrayList<>();

        for (LogEvent event : events) {

            try {
                LogDocument doc = objectMapper.convertValue(event, LogDocument.class);

                if (isValid(doc)) {
                    validDocs.add(doc);
                } else {
                    saveToFailedStore(event, "VALIDATION_FAILED", "Missing required fields");
                }

            } catch (Exception ex) {
                saveToFailedStore(event, "MAPPING_FAILED", ex.getMessage());
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
            return;
        }

        catch (BulkOperationException ex) {

            Set<Integer> failedIndexes = ex.getErrors()
                    .stream()
                    .map(BulkWriteError::getIndex)
                    .collect(Collectors.toSet());

            for (int i = 0; i < validDocs.size(); i++) {

                if (!failedIndexes.contains(i)) continue;

                LogDocument failedDoc = validDocs.get(i);

                try {
                    // retry single insert
                    mongoTemplate.save(failedDoc);

                } catch (Exception retryEx) {

                    saveToFailedStore(
                            failedDoc,
                            "MONGO_FINAL_FAILURE",
                            retryEx.getMessage()
                    );
                }
            }
        }
    }


    private boolean isValid(LogDocument doc) {

        return doc.getId() != null &&
                doc.getTimestamp() != null &&
                doc.getServiceName() != null;
    }

    private void saveToFailedStore(Object data, String reason, String error) {

        try {
            String payload = objectMapper.writeValueAsString(data);

            FailedLog failedLog = new FailedLog(
                    payload,
                    reason,
                    error
            );

            failedLogRepository.save(failedLog);

        } catch (Exception ex) {
            try {
                FailedLog fallback = new FailedLog(
                        String.valueOf(data),
                        "SERIALIZATION_FAILED",
                        ex.getMessage()
                );

                failedLogRepository.save(fallback);

            } catch (Exception fatal) {
                // absolutely critical failure
                System.err.println("CRITICAL: failed_logs storage broken");
                fatal.printStackTrace();
            }
        }
    }
}