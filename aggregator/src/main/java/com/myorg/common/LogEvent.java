package com.myorg.common;

import java.time.Instant;
import java.util.Map;

public record LogEvent(
        String id,
        Instant timestamp,
        String serviceName,
        String logLevel,
        String traceId,
        String message,
        Long durationMs,
        Map<String, Object> metadata
) {
}
