package com.contdistrapp.refdata.service;

import com.contdistrapp.refdata.config.role.ConditionalOnRefdataRole;
import com.contdistrapp.refdata.domain.UpdateCommand;
import com.contdistrapp.refdata.persistence.PlatformRepository;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import java.time.Instant;

@Component
@ConditionalOnProperty(prefix = "refdata.kafka", name = {"enabled", "external-enabled"}, havingValue = "true")
@ConditionalOnRefdataRole({"apply-service"})
public class ExternalKafkaAdapter {

    private static final Logger log = LoggerFactory.getLogger(ExternalKafkaAdapter.class);

    private final ObjectMapper objectMapper;
    private final PlatformRepository repository;
    private final CommandPublisher commandPublisher;

    public ExternalKafkaAdapter(
            ObjectMapper objectMapper,
            PlatformRepository repository,
            CommandPublisher commandPublisher
    ) {
        this.objectMapper = objectMapper;
        this.repository = repository;
        this.commandPublisher = commandPublisher;
    }

    @KafkaListener(topics = "${refdata.kafka.external-topic}", groupId = "${refdata.kafka.group-id}-external")
    public void onExternalMessage(String payload) {
        try {
            UpdateCommand incoming = objectMapper.readValue(payload, UpdateCommand.class);
            UpdateCommand normalized = normalize(incoming);
            repository.createUpdateRequestIfAbsent(normalized);
            commandPublisher.publish(normalized);
        } catch (Exception ex) {
            log.error("Failed to process external Kafka event", ex);
        }
    }

    private UpdateCommand normalize(UpdateCommand incoming) {
        return new UpdateCommand(
                incoming.eventId(),
                incoming.tenantId(),
                "KAFKA",
                incoming.dictCode(),
                incoming.eventType(),
                incoming.sourceRevision(),
                incoming.snapshotId(),
                incoming.chunkIndex(),
                incoming.chunksTotal(),
                incoming.occurredAt() == null ? Instant.now() : incoming.occurredAt(),
                incoming.items()
        );
    }
}
