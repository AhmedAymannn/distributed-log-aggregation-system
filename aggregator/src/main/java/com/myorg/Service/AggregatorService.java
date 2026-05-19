package com.myorg.Service;

import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.stereotype.Service;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.List;

@Service
public class AggregatorService {

    @KafkaListener(topics = "app-logs", groupId = "aggregator-group")
    public void processLogs(List <String> logs) {


    }

}
