package com.myorg;


import com.myorg.Service.AggregatorService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class Aggregator {

    public static void main(String[] args) {
        SpringApplication.run(Aggregator.class,args);

    }
}