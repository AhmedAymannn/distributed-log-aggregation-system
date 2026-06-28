package com.myorg.Validator;

import com.myorg.document.LogDocument;
import org.springframework.stereotype.Component;

@Component
public class LogValidator {

    public boolean isValid(LogDocument doc) {
        return doc.getTimestamp() != null &&
                doc.getServiceName() != null &&
                !doc.getServiceName().isBlank();
    }

    public String getFailureReason(LogDocument doc) {
        if (doc.getTimestamp() == null)   return "MISSING_TIMESTAMP";
        if (doc.getServiceName() == null) return "MISSING_SERVICE_NAME";
        if (doc.getServiceName().isBlank()) return "BLANK_SERVICE_NAME";
        return "UNKNOWN";
    }
}