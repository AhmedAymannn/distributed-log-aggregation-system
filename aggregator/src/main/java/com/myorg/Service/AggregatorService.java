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


        }



    private boolean isValid(LogDocument doc) {

        return doc.getId() != null &&
                doc.getTimestamp() != null &&
                doc.getServiceName() != null;
    }

    private void sendToDlq (){

    }
    private void handleBulkFailure(){
        
    }
}



