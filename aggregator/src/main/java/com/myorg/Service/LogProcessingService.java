package com.myorg.Service;

import com.myorg.mapper.LogMapper;
import com.myorg.Service.LogDlqService;
import com.myorg.Service.LogStorageService;
import com.myorg.Validator.LogValidator;
import com.myorg.common.LogEvent;
import com.myorg.document.LogDocument;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.List;

@Service
public class LogProcessingService {

    private final LogMapper mapper;
    private final LogValidator validator;
    private final LogStorageService logStorageService;
    private final LogDlqService dlqService;
    private static final Logger log = LoggerFactory.getLogger(LogProcessingService.class);

    public LogProcessingService(LogMapper mapper,
                                LogValidator validator,
                                LogStorageService logStorageService,
                                LogDlqService dlqService) {
        this.mapper = mapper;
        this.validator = validator;
        this.logStorageService = logStorageService;
        this.dlqService = dlqService;
    }

    public void processBatch(List<LogEvent> events) {
        if (events == null || events.isEmpty()) return;

        List<LogDocument> validDocs = new ArrayList<>();

        for (LogEvent event : events) {
            try {
                LogDocument doc = mapper.toDocument(event);
                if (validator.isValid(doc)) {
                    validDocs.add(doc);
                } else {
                    dlqService.send(event, validator.getFailureReason(doc));
                }
            } catch (Exception e) {
                dlqService.send(event, "MAPPING_FAILED");
            }
        }

        if (!validDocs.isEmpty()) {
            logStorageService.saveAll(validDocs);
        }

        log.info("Batch processed. Valid: {}, Invalid: {}",
                validDocs.size(), events.size() - validDocs.size());
    }
}
