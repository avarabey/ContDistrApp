package com.contdistrapp.refdata.apply;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.ConfigurationPropertiesScan;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.scheduling.annotation.EnableScheduling;

@SpringBootApplication(scanBasePackages = "com.contdistrapp.refdata")
@ConfigurationPropertiesScan(basePackages = "com.contdistrapp.refdata")
@EnableScheduling
@EnableKafka
public class ApplyServiceApplication {

    public static void main(String[] args) {
        SpringApplication.run(ApplyServiceApplication.class, args);
    }
}
