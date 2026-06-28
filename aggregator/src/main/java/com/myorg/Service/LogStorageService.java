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

import java.util.*;
import java.util.stream.Collectors;

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
        BulkOperations bulkOps = mongoTemplate.bulkOps(BulkOperations.BulkMode.UNORDERED, LogDocument.class);

        docs.forEach(doc -> bulkOps.upsert(
                new Query(Criteria.where("_id").is(doc.getId())),
                buildUpdate(doc)
        ));

        try {
            bulkOps.execute();
        } catch (BulkOperationException ex) {
            // Slicing: Directly access the failed index using the driver's report
            ex.getErrors().forEach(error -> {
                LogDocument failed = docs.get(error.getIndex());
                dlqService.send(failed, "WRITE_FAILURE: " + error.getMessage());
            });
        } catch (Exception e) {
            // Surface system-level exceptions to trigger Kafka's retry mechanism
            throw new RuntimeException("System error, triggering batch retry", e);
        }
    }
    private Update buildUpdate(LogDocument doc) {
        return new Update()
                .setOnInsert("_id", doc.getId()) // Only sets ID if it's a new insert
                .set("timestamp", doc.getTimestamp())
                .set("serviceName", doc.getServiceName())
                .set("logLevel", doc.getLogLevel())
                .set("traceId", doc.getTraceId())
                .set("message", doc.getMessage())
                .set("durationMs", doc.getDurationMs())
                .set("metadata", doc.getMetadata());
    }


}
