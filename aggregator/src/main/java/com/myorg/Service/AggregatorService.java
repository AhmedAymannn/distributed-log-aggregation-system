package com.myorg.Service;
import com.myorg.document.LogDocument;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.myorg.repository.LogRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import java.util.ArrayList;
import java.util.List;
@Service
public class AggregatorService {

    private static final Logger log = LoggerFactory.getLogger(AggregatorService.class);
    private final ObjectMapper objectMapper;

    private final LogRepository logRepository ;

    public AggregatorService(ObjectMapper objectMapper, LogRepository logRepository) {
        this.objectMapper = objectMapper;
        this.logRepository = logRepository;
    }

    public void processBatch(List<String> messages) {
        List<LogDocument> validLogs = new ArrayList<>(messages.size());
        for (String message : messages) {
            try {
                LogDocument logDocument =
                        objectMapper.readValue(message, LogDocument.class);

                if (isValid(logDocument)) {
                    validLogs.add(logDocument);
                }

            } catch (Exception ex) {
                log.error("Failed to parse log message", ex);
            }
        }

        if (!validLogs.isEmpty()) {
            logRepository.saveAll(validLogs);
            log.info("Saved {} logs to MongoDB", validLogs.size());
        }
    }

    private boolean isValid(LogDocument log) {
        return log != null
                && log.timestamp() != null
                && log.serviceName() != null
                && log.logLevel() != null
                && log.message() != null;
    }

}