package com.myorg.Consumer;


import org.springframework.kafka.annotation.KafkaListener;

@KafkaListener(topics = "app-logs" , groupId = "aggregator-group")
public class LogKafkaListener {
    public void listen(String log) {
        // You can ONLY receive one String at a time here.
        // No List<String> allowed!
        repository.save(new LogEntity(log));
    }
}
