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
        if (docs == null || docs.isEmpty()) return;

        // Build ID map — source of truth for this batch
        Map<String, LogDocument> docById = docs.stream()
                .collect(Collectors.toMap(LogDocument::getId, doc -> doc));

        // Phase 1 — attempt bulk upsert
        Set<String> failedIds = executeBulk(docs);

        // Phase 2 — handle failures individually
        if (!failedIds.isEmpty()) {
            log.warn("{} documents failed bulk write. Attempting individual retry.", failedIds.size());
            failedIds.forEach(id -> {
                LogDocument failed = docById.get(id);  // ← correct document always
                retryOrDlq(failed, "BULK_WRITE_FAILED");
            });
        }
    }

    private Set<String> executeBulk(List<LogDocument> docs) {
        BulkOperations bulkOps = mongoTemplate
                .bulkOps(BulkOperations.BulkMode.UNORDERED, LogDocument.class);

        for (LogDocument doc : docs) {
            Query query = new Query(Criteria.where("_id").is(doc.getId()));
            Update update = new Update()
                    .setOnInsert("_id",       doc.getId())
                    .set("timestamp",         doc.getTimestamp())
                    .set("serviceName",       doc.getServiceName())
                    .set("logLevel",          doc.getLogLevel())
                    .set("traceId",           doc.getTraceId())
                    .set("message",           doc.getMessage())
                    .set("durationMs",        doc.getDurationMs())
                    .set("metadata",          doc.getMetadata());
            bulkOps.upsert(query, update);
        }

        try {
            bulkOps.execute();
            log.info("Bulk write successful. {} documents saved.", docs.size());
            return Collections.emptySet();  // no failures

        } catch (BulkOperationException ex) {
            // extract failed ids by querying mongo for what actually saved
            return reconcileFailures(docs, ex);

        } catch (Exception e) {
            // MongoDB is down — rethrow so Kafka retries entire batch
            log.error("Fatal MongoDB error: {}", e.getMessage());
            throw e;
        }
    }

    private Set<String> reconcileFailures(List<LogDocument> docs, BulkOperationException ex) {
        log.warn("Bulk partial failure. {} errors reported.", ex.getErrors().size());

        // collect all ids we tried to save
        Set<String> attemptedIds = docs.stream()
                .map(LogDocument::getId)
                .collect(Collectors.toSet());

        // ask MongoDB which ones actually made it
        Query checkQuery = new Query(Criteria.where("_id").in(attemptedIds));
        checkQuery.fields().include("_id");  // only fetch ids, not full documents

        Set<String> savedIds = mongoTemplate
                .find(checkQuery, LogDocument.class)
                .stream()
                .map(LogDocument::getId)
                .collect(Collectors.toSet());

        // the ones that didn't make it = attempted - saved
        attemptedIds.removeAll(savedIds);
        return attemptedIds;  // these are the real failed ids
    }

    private void retryOrDlq(LogDocument doc, String reason) {
        if (doc == null) {
            log.error("Cannot retry null document. Reason: {}", reason);
            return;
        }
        try {
            mongoTemplate.save(doc);
            log.info("Single retry successful for id: {}", doc.getId());
        } catch (Exception e) {
            log.error("Single retry failed for id: {}. Sending to DLQ.", doc.getId());
            dlqService.send(doc, "MONGO_FINAL_FAILURE: " + reason);
        }
    }
}
