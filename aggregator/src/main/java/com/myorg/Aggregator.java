package com.myorg;


import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class Aggregator {
    public static void main(String[] args) {
        SpringApplication.run(Aggregator.class, args);
        System.out.println("======================");
        System.out.println("Hello from Aggregator");
        System.out.println("======================");
    }
}