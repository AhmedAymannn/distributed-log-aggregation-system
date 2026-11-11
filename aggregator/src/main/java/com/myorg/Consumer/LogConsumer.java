package com.myorg.Consumer;

import com.myorg.Service.LogProcessingService;
import com.myorg.common.LogEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

import java.util.List;

@Component
public class LogConsumer {

    private final LogProcessingService logprocessingService;
    private static final Logger log = LoggerFactory.getLogger(LogConsumer.class);

    public LogConsumer(LogProcessingService logprocessingService) {
        this.logprocessingService = logprocessingService;
    }

    @KafkaListener(
            topics = "app-logs",
            containerFactory = "kafkaListenerContainerFactory"
    )
    public void consume(List<LogEvent> events, Acknowledgment ack) {
        try {
            log.info("Received batch of {} logs", events.size());
            logprocessingService.processBatch(events);
            ack.acknowledge();
        } catch (Exception e) {
            log.error("Fatal error processing batch. Triggering retry.", e);
            throw e;
        }
    }
}