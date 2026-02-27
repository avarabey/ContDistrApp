package com.contdistrapp.refdata;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.ConfigurationPropertiesScan;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.scheduling.annotation.EnableScheduling;

@SpringBootApplication
@ConfigurationPropertiesScan
@EnableScheduling
@EnableKafka
public class RefdataPlatformApplication {

    public static void main(String[] args) {
        SpringApplication.run(RefdataPlatformApplication.class, args);
    }
}
