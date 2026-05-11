package com.myorg;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class ProducerOneApplication {
    public static void main(String[]args){
        SpringApplication.run(ProducerOneApplication.class, args);
        System.out.println("======================");
        System.out.println("Hello from producer 1 ");
        System.out.println("======================");

    }
}
