package com.myorg;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class ProducerOneApplication {
    private static final Logger log = LoggerFactory.getLogger(ProducerOneApplication.class);

    public static void main(String[]args){
        SpringApplication.run(ProducerOneApplication.class, args);
        System.out.println("======================");
        System.out.println("Hello from producer 1 ");
        System.out.println("======================");
        for(int i =0 ; i < 10 ; i++){
            log.info("Log simulation from prod_1... ");
            log.info("==========================");
        }
    }
}
