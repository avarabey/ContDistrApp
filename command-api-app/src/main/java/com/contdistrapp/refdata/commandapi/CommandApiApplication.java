package com.contdistrapp.refdata.commandapi;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.ConfigurationPropertiesScan;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.scheduling.annotation.EnableScheduling;

@SpringBootApplication(scanBasePackages = "com.contdistrapp.refdata")
@ConfigurationPropertiesScan(basePackages = "com.contdistrapp.refdata")
@EnableScheduling
@EnableKafka
public class CommandApiApplication {

    public static void main(String[] args) {
        SpringApplication.run(CommandApiApplication.class, args);
    }
}
