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
import org.springframework.stereotype.Service;

import java.util.List;

@Service
public class LogStorageService {

    private final MongoTemplate mongoTemplate;
    private final LogDlqService dlqService;
    private static final Logger log = LoggerFactory.getLogger(LogStorageService.class);

    public LogStorageService(MongoTemplate mongoTemplate,
                             LogDlqService dlqService) {
        this.mongoTemplate = mongoTemplate;
        this.dlqService = dlqService;
    }

    public void saveAll(List<LogDocument> docs) {
        if (docs == null || docs.isEmpty()) return;

        BulkOperations bulkOps = mongoTemplate
                .bulkOps(BulkOperations.BulkMode.ORDERED, LogDocument.class);

        docs.forEach(doc -> bulkOps.upsert(
                new Query(Criteria.where("_id").is(doc.getId())),
                buildUpdate(doc)
        ));

        try {
            bulkOps.execute();
            log.info("Bulk write successful. {} documents saved.", docs.size());

        } catch (BulkOperationException ex) {
            int failedIndex = ex.getErrors().get(0).getIndex();
            log.error("Bulk write failed at index {} of {}. Triggering Kafka retry.",
                    failedIndex, docs.size());
            throw ex;  // Kafka redelivers whole batch — deterministic id is safe

        } catch (Exception e) {
            log.error("Fatal MongoDB error. Triggering Kafka retry. Reason: {}", e.getMessage());
            throw e;
        }
    }

    private Update buildUpdate(LogDocument doc) {
        return new Update()
                .setOnInsert("_id", doc.getId())
                .set("timestamp", doc.getTimestamp())
                .set("serviceName", doc.getServiceName())
                .set("logLevel", doc.getLogLevel())
                .set("traceId", doc.getTraceId())
                .set("message", doc.getMessage())
                .set("durationMs", doc.getDurationMs())
                .set("metadata", doc.getMetadata());
    }
}