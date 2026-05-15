package com.myorg.Service;


import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

import java.util.List;

@Service
public class AggregatorService {


    @KafkaListener(topics =  "app-logs" , groupId = "aggregator-group" ,
            containerFactory = "kafkaListenerContainerFactory" )
    public void processLogs() {
        //process
        //saving
        saveToDB();

    }
    
    public void saveToDB (){
        // saving to dataBase
    }
}
