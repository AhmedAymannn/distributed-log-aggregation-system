package com.myorg.Controller;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class TestController {

    private static final Logger log = LoggerFactory.getLogger(TestController.class);

    @GetMapping("/send")
    public String sendManualLog(@RequestParam(defaultValue = "Manual Test") String message) {
        // This line triggers the Logback-Kafka-Appender in your XML
        log.info("Sending message to Kafka: {}", message);

        return "Log sent to Kafka topic 'app-logs': " + message;
    }
}