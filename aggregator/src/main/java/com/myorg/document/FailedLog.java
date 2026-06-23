package com.myorg.document;

import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.mapping.Document;

import java.time.Instant;

@Document("failed_logs")
public class FailedLog {

    @Id
    private String id;

    private String payload;   // JSON string
    private String reason;
    private String error;
    private Instant timestamp;

    public FailedLog() {}

    public FailedLog(String payload, String reason, String error) {
        this.payload = payload;
        this.reason = reason;
        this.error = error;
        this.timestamp = Instant.now();
    }

    public String getPayload() {
        return payload;
    }

    public void setPayload(String payload) {
        this.payload = payload;
    }

    public String getReason() {
        return reason;
    }

    public void setReason(String reason) {
        this.reason = reason;
    }

    public String getError() {
        return error;
    }

    public void setError(String error) {
        this.error = error;
    }

    public Instant getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(Instant timestamp) {
        this.timestamp = timestamp;
    }
}