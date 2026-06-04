package com.myorg.document;

import com.fasterxml.jackson.annotation.JsonAnySetter;
import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.index.Indexed;
import org.springframework.data.mongodb.core.mapping.Document;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;

@Document(collection = "logs")
public class LogDocument {

        @Id
        private String id;
        @Indexed(name = "ttl_index", expireAfter = "7d")
        private Instant timestamp;
        @Indexed
        private String serviceName;
        @Indexed
        private String logLevel;
        @Indexed
        private String traceId;
        private String message;
        private Long durationMs;
        private Map<String, Object> metadata = new HashMap<>();

        public LogDocument() {}

        @JsonAnySetter
        public void handleUnknownField(String key, Object value) {
                this.metadata.put(key, value);
        }

        public String getId() { return id; }
        public void setId(String id) { this.id = id; }

        public Instant getTimestamp() { return timestamp; }
        public void setTimestamp(Instant timestamp) { this.timestamp = timestamp; }

        public String getServiceName() { return serviceName; }
        public void setServiceName(String serviceName) { this.serviceName = serviceName; }

        public String getLogLevel() { return logLevel; }
        public void setLogLevel(String logLevel) { this.logLevel = logLevel; }

        public String getTraceId() { return traceId; }
        public void setTraceId(String traceId) { this.traceId = traceId; }

        public String getMessage() { return message; }
        public void setMessage(String message) { this.message = message; }

        public Long getDurationMs() { return durationMs; }
        public void setDurationMs(Long durationMs) { this.durationMs = durationMs; }

        public Map<String, Object> getMetadata() { return metadata; }
        public void setMetadata(Map<String, Object> metadata) { this.metadata = metadata; }
}