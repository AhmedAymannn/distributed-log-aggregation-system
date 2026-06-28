package Mapper;

import com.myorg.common.LogEvent;
import com.myorg.document.LogDocument;
import org.springframework.stereotype.Component;

import java.util.UUID;

@Component
public class LogMapper {

    public LogDocument toDocument(LogEvent event) {
        if (event == null) {
            throw new IllegalArgumentException("LogEvent cannot be null");
        }

        LogDocument doc = new LogDocument();
        doc.setId(generateId(event));
        doc.setTimestamp(event.timestamp());
        doc.setServiceName(event.serviceName());
        doc.setLogLevel(event.logLevel());
        doc.setTraceId(event.traceId());
        doc.setMessage(event.message());
        doc.setDurationMs(event.durationMs());

        if (event.metadata() != null) {
            doc.setMetadata(event.metadata());
        }

        return doc;
    }

    private String generateId(LogEvent event) {
        String raw = event.serviceName()
                + event.timestamp()
                + event.traceId();
        return UUID.nameUUIDFromBytes(raw.getBytes()).toString();
    }
}