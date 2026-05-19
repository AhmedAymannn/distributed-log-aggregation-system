package com.myorg.document;

import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.index.Indexed;
import org.springframework.data.mongodb.core.mapping.Document;
import java.time.Instant;
import java.util.Map;

@Document
public record LogDocument(
        @Id
        String id,

        @Indexed(name = "ttl_index", expireAfter = "7d")
        Instant timestamp, // Changed from Date to Instant

        @Indexed
        String serviceName,

        @Indexed
        String logLevel,

        @Indexed
        String traceId,

        String message,
        Long durationMs,
        Map<String, Object> metadata
) {

}