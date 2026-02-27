package com.contdistrapp.refdata.service;

import com.contdistrapp.refdata.config.RefDataProperties;
import com.contdistrapp.refdata.config.role.ConditionalOnRefdataRole;
import com.contdistrapp.refdata.domain.UpdateCommand;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

@Component
@ConditionalOnProperty(prefix = "refdata.kafka", name = "enabled", havingValue = "true")
@ConditionalOnRefdataRole({"command-api", "apply-service"})
public class KafkaCommandPublisher implements CommandPublisher {

    private final KafkaTemplate<String, String> kafkaTemplate;
    private final ObjectMapper objectMapper;
    private final RefDataProperties properties;

    public KafkaCommandPublisher(
            KafkaTemplate<String, String> kafkaTemplate,
            ObjectMapper objectMapper,
            RefDataProperties properties
    ) {
        this.kafkaTemplate = kafkaTemplate;
        this.objectMapper = objectMapper;
        this.properties = properties;
    }

    @Override
    public void publish(UpdateCommand command) {
        try {
            String payload = objectMapper.writeValueAsString(command);
            kafkaTemplate.send(properties.getKafka().getCommandsTopic(), command.partitionKey(), payload);
        } catch (JsonProcessingException e) {
            throw new IllegalStateException("Unable to serialize command for Kafka", e);
        }
    }
}
