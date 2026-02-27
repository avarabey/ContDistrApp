package com.contdistrapp.refdata.service;

import com.contdistrapp.refdata.bus.InvalidationBus;
import com.contdistrapp.refdata.config.role.ConditionalOnRefdataRole;
import com.contdistrapp.refdata.domain.InvalidationEvent;
import com.contdistrapp.refdata.persistence.OutboxEventRecord;
import com.contdistrapp.refdata.persistence.PlatformRepository;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.util.List;

@Service
@ConditionalOnRefdataRole({"outbox-relay"})
public class OutboxRelayService {

    private static final Logger log = LoggerFactory.getLogger(OutboxRelayService.class);

    private final PlatformRepository repository;
    private final InvalidationBus invalidationBus;
    private final RefDataTimeouts timeouts;
    private final ObjectMapper objectMapper;

    public OutboxRelayService(
            PlatformRepository repository,
            InvalidationBus invalidationBus,
            RefDataTimeouts timeouts,
            ObjectMapper objectMapper
    ) {
        this.repository = repository;
        this.invalidationBus = invalidationBus;
        this.timeouts = timeouts;
        this.objectMapper = objectMapper;
    }

    @Scheduled(fixedDelayString = "#{@refDataTimeouts.outboxPollIntervalMs()}")
    public void relay() {
        List<OutboxEventRecord> batch = repository.fetchUnpublishedOutbox(timeouts.outboxBatchSize());
        for (OutboxEventRecord record : batch) {
            try {
                InvalidationEvent event = objectMapper.readValue(record.payload(), InvalidationEvent.class);
                invalidationBus.publish(event);
                repository.markOutboxPublished(record.id());
            } catch (Exception ex) {
                log.error("Failed to relay outbox event id={} tenant={} dict={}", record.id(), record.tenantId(), record.dictCode(), ex);
            }
        }
    }
}
