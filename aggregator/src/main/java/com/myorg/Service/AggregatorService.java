package com.myorg.Service;
import com.myorg.document.LogDocument;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.mongodb.BulkOperationException;
import org.springframework.data.mongodb.core.BulkOperations;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;
import org.springframework.stereotype.Service;
import java.util.List;

@Service
public class AggregatorService {

    private static final Logger log = LoggerFactory.getLogger(AggregatorService.class);
    private final ObjectMapper objectMapper;
    private final MongoTemplate mongoTemplate;

    public AggregatorService(ObjectMapper objectMapper, MongoTemplate mongoTemplate) {
        this.objectMapper = objectMapper;
        this.mongoTemplate = mongoTemplate;
    }

    public void processBatch(List<String> messages) {
        if (messages == null || messages.isEmpty()) return;
        BulkOperations bulkOps = mongoTemplate.bulkOps(BulkOperations.BulkMode.UNORDERED, LogDocument.class);
        int validLogCount = 0;

        for (String message : messages) {
            try {
                LogDocument logDocument = objectMapper.readValue(message, LogDocument.class);

                if (isValid(logDocument)) {
                    Query query = new Query(Criteria.where("id").is(logDocument.getId()));

                    Update update = new Update()
                            .set("timestamp", logDocument.getTimestamp())
                            .set("serviceName", logDocument.getServiceName())
                            .set("logLevel", logDocument.getLogLevel())
                            .set("traceId", logDocument.getTraceId())
                            .set("message", logDocument.getMessage())
                            .set("durationMs", logDocument.getDurationMs())
                            .set("metadata", logDocument.getMetadata());

                    bulkOps.upsert(query, update);
                    validLogCount++;
                } else {
                    log.warn("Skipped log due to missing mandatory structural fields: {}", message);
                }

            } catch (Exception ex) {
                log.error("Malformed JSON syntax skipped: {}", ex.getMessage());
            }
        }

        if (validLogCount > 0) {
            try {
                bulkOps.execute();
                log.info("Successfully processed batch of {} logs.", validLogCount);
            } catch (BulkOperationException boe) {
                boe.getErrors().forEach(error -> {
                    log.error("MongoDB isolated and skipped a bad log! Reason: {}", error.getMessage());
                });
            } catch (Exception ex) {
                log.error("CRITICAL: Network/Database outage occurred during bulk save!", ex);
                throw ex;
            }
        }
    }

    private boolean isValid(LogDocument log) {
        return log != null
                && log.getId() != null
                && log.getTimestamp() != null
                && log.getServiceName() != null
                && log.getLogLevel() != null
                && log.getMessage() != null;
    }
}