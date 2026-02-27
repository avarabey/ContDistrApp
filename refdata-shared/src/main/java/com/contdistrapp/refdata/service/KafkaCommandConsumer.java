package com.contdistrapp.refdata.service;

import com.contdistrapp.refdata.config.role.ConditionalOnRefdataRole;
import com.contdistrapp.refdata.domain.UpdateCommand;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
@ConditionalOnProperty(prefix = "refdata.kafka", name = "enabled", havingValue = "true")
@ConditionalOnRefdataRole({"apply-service"})
public class KafkaCommandConsumer {

    private static final Logger log = LoggerFactory.getLogger(KafkaCommandConsumer.class);

    private final ObjectMapper objectMapper;
    private final CommandDispatcher dispatcher;

    public KafkaCommandConsumer(ObjectMapper objectMapper, CommandDispatcher dispatcher) {
        this.objectMapper = objectMapper;
        this.dispatcher = dispatcher;
    }

    @KafkaListener(topics = "${refdata.kafka.commands-topic}", groupId = "${refdata.kafka.group-id}")
    public void onMessage(String payload) {
        try {
            UpdateCommand command = objectMapper.readValue(payload, UpdateCommand.class);
            dispatcher.dispatch(command);
        } catch (Exception ex) {
            log.error("Failed to consume command from Kafka", ex);
        }
    }
}
