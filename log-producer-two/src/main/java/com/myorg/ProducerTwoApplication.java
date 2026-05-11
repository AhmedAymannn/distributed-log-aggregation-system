package com.myorg;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class ProducerTwoApplication {
    public static void main(String[] args) {
        SpringApplication.run(ProducerTwoApplication.class, args);
        System.out.println("======================");
        System.out.println("Hello from producer 2 ");
        System.out.println("======================");

    }
    }
