package com.myorg.Service;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import java.util.List;

@Component
public class LogConsumer {

    private static final Logger log = LoggerFactory.getLogger(LogConsumer.class);
    private final AggregatorService aggregatorService;

    public LogConsumer(AggregatorService aggregatorService) {
        this.aggregatorService = aggregatorService;
    }
    @KafkaListener(
            topics = "app-logs",
            containerFactory = "kafkaListenerContainerFactory"
    )
    public void consume(List<String> messages) {
        log.info("Received batch with {} logs", messages.size());
        aggregatorService.processBatch(messages);
    }
}