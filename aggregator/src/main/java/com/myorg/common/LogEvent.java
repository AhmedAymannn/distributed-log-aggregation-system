package com.myorg.common;

import java.time.Instant;
import java.util.Map;

public record LogEvent(
        Instant timestamp,
        String logLevel,
        String message,
        String traceId,
        String serviceName,
        Map<String, Object> metadata
) {}
