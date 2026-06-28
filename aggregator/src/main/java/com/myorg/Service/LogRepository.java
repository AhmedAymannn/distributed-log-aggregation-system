package com.myorg.Service;

import com.myorg.document.LogDocument;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.mongodb.BulkOperationException;
import org.springframework.data.mongodb.core.BulkOperations;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Update;

import org.springframework.data.mongodb.core.query.Query;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class LogRepository {

    private final MongoTemplate mongoTemplate;
    private final LogDlqService dlqService;
    private static final Logger log = LoggerFactory.getLogger(LogRepository.class);

    public LogRepository(MongoTemplate mongoTemplate,
                                 LogDlqService dlqService) {
        this.mongoTemplate = mongoTemplate;
        this.dlqService = dlqService;
    }

    public void saveAll(List<LogDocument> docs) {
        if (docs.isEmpty()) return;

        Map<String, LogDocument> docById = new HashMap<>();
        docs.forEach(doc -> docById.put(doc.getId(), doc));

        BulkOperations bulkOps = mongoTemplate
                .bulkOps(BulkOperations.BulkMode.UNORDERED, LogDocument.class);

        for (LogDocument doc : docs) {
            Query query = new Query(Criteria.where("id").is(doc.getId()));
            Update update = new Update()
                    .set("timestamp",   doc.getTimestamp())
                    .set("serviceName", doc.getServiceName())
                    .set("logLevel",    doc.getLogLevel())
                    .set("traceId",     doc.getTraceId())
                    .set("message",     doc.getMessage())
                    .set("durationMs",  doc.getDurationMs())
                    .set("metadata",    doc.getMetadata());
            bulkOps.upsert(query, update);
        }

        try {
            bulkOps.execute();
        } catch (BulkOperationException ex) {
            ex.getErrors().forEach(error -> {
                try {
                    LogDocument failed = docs.get(error.getIndex());
                    retryOrDlq(failed, error.getMessage());
                } catch (IndexOutOfBoundsException e) {
                    log.error("Could not reconcile bulk error at index {}", error.getIndex());
                }
            });
        } catch (Exception e) {
            log.error("Fatal MongoDB error: {}", e.getMessage());
            throw e;  // rethrow → Kafka retries the batch
        }
    }

    private void retryOrDlq(LogDocument doc, String reason) {
        try {
            mongoTemplate.save(doc);
        } catch (Exception e) {
            dlqService.send(doc, "MONGO_FINAL_FAILURE: " + reason);
        }
    }
}
