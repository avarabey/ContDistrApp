package com.contdistrapp.refdata.service;

import com.contdistrapp.refdata.config.role.ConditionalOnRefdataRole;
import com.contdistrapp.refdata.domain.EventType;
import com.contdistrapp.refdata.domain.InvalidationEvent;
import com.contdistrapp.refdata.domain.UpdateCommand;
import com.contdistrapp.refdata.domain.UpdateItem;
import com.contdistrapp.refdata.persistence.DictionaryMetaRecord;
import com.contdistrapp.refdata.persistence.DictionaryProvider;
import com.contdistrapp.refdata.persistence.PlatformRepository;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;
import java.util.List;

@Service
@ConditionalOnRefdataRole({"apply-service"})
public class ApplyProcessor {

    private static final Logger log = LoggerFactory.getLogger(ApplyProcessor.class);

    private final PlatformRepository repository;
    private final DictionaryProvider dictionaryProvider;
    private final DictionaryRegistry dictionaryRegistry;
    private final ObjectMapper objectMapper;

    public ApplyProcessor(
            PlatformRepository repository,
            DictionaryProvider dictionaryProvider,
            DictionaryRegistry dictionaryRegistry,
            ObjectMapper objectMapper
    ) {
        this.repository = repository;
        this.dictionaryProvider = dictionaryProvider;
        this.dictionaryRegistry = dictionaryRegistry;
        this.objectMapper = objectMapper;
    }

    @Transactional
    public void process(UpdateCommand command) {
        dictionaryRegistry.required(command.dictCode());
        repository.createUpdateRequestIfAbsent(command);

        if (!repository.tryMarkProcessed(command)) {
            log.debug("Duplicate event ignored eventId={} tenant={}", command.eventId(), command.tenantId());
            return;
        }

        if (isStaleRevision(command)) {
            repository.markUpdateFailed(command.tenantId(), command.eventId(), "Stale sourceRevision");
            return;
        }

        if (command.eventType() == EventType.SNAPSHOT && command.isChunkedSnapshot()) {
            handleChunkedSnapshot(command);
            return;
        }

        long version = repository.allocateNextVersion(command.tenantId(), command.dictCode(), command.sourceRevision());
        applyByType(command, command.items(), version);

        repository.markUpdateCommitted(command.tenantId(), command.eventId(), version);
        repository.insertOutboxEvent(command.tenantId(), command.eventId(), command.dictCode(), version, serializeInvalidation(command, version));
    }

    @Transactional
    public void fail(UpdateCommand command, String message) {
        repository.createUpdateRequestIfAbsent(command);
        repository.markUpdateFailed(command.tenantId(), command.eventId(), message == null ? "Unknown error" : message);
    }

    private boolean isStaleRevision(UpdateCommand command) {
        if (command.sourceRevision() == null) {
            return false;
        }
        DictionaryMetaRecord meta = repository.dictionaryMeta(command.tenantId(), command.dictCode());
        return meta.lastSourceRevision() != null && command.sourceRevision() <= meta.lastSourceRevision();
    }

    private void handleChunkedSnapshot(UpdateCommand command) {
        repository.saveSnapshotChunk(command);
        int chunks = repository.countSnapshotChunks(command.tenantId(), command.dictCode(), command.snapshotId());
        if (chunks < command.chunksTotal()) {
            return;
        }

        List<UpdateItem> snapshotItems = repository.loadSnapshotItems(command.tenantId(), command.dictCode(), command.snapshotId());
        long version = repository.allocateNextVersion(command.tenantId(), command.dictCode(), command.sourceRevision());

        applyByType(command, snapshotItems, version);

        repository.clearSnapshotChunks(command.tenantId(), command.dictCode(), command.snapshotId());
        repository.markSnapshotCommitted(command.tenantId(), command.dictCode(), command.snapshotId(), version);
        repository.markUpdateCommitted(command.tenantId(), command.eventId(), version);
        repository.insertOutboxEvent(command.tenantId(), command.eventId(), command.dictCode(), version, serializeInvalidation(command, version));
    }

    private void applyByType(UpdateCommand command, List<UpdateItem> items, long version) {
        if (command.eventType() == EventType.DELTA) {
            dictionaryProvider.applyDelta(command.tenantId(), command.dictCode(), items, command, version);
        } else {
            dictionaryProvider.applySnapshot(command.tenantId(), command.dictCode(), items, command, version);
        }
    }

    private String serializeInvalidation(UpdateCommand command, long version) {
        InvalidationEvent event = new InvalidationEvent(
                command.eventId(),
                command.tenantId(),
                command.dictCode(),
                version,
                Instant.now()
        );
        try {
            return objectMapper.writeValueAsString(event);
        } catch (JsonProcessingException e) {
            throw new IllegalStateException("Unable to serialize invalidation", e);
        }
    }
}
