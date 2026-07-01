package com.myorg.common;


import com.mongodb.MongoBulkWriteException;
import com.mongodb.bulk.BulkWriteError;
import com.myorg.document.LogDocument;
import org.springframework.stereotype.Component;
import com.myorg.Service.LogDlqService;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

@Component
public class ErrorHandler {
    private final LogDlqService logDlqService ;

    public ErrorHandler(LogDlqService logDlqService) {
        this.logDlqService = logDlqService;
    }
    private List<LogDocument> handleBulkFailure(
            List<LogDocument> docs,
            BulkWriteError error) {

        int failedIndex = error.getIndex();

        LogDocument failedDocument = docs.get(failedIndex);

        logDlqService.send(
                failedDocument,
                error.getMessage());

        if (failedIndex + 1 >= docs.size()) {
            return Collections.emptyList();
        }

        return new ArrayList<>(
                docs.subList(
                        failedIndex + 1,
                        docs.size()));
    }
}
