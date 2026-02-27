package com.contdistrapp.refdata.service;

import com.contdistrapp.refdata.api.UpdateCommandRequest;
import com.contdistrapp.refdata.api.UpdateStatusResponse;
import com.contdistrapp.refdata.api.UpdateSubmissionResponse;
import com.contdistrapp.refdata.config.role.ConditionalOnRefdataRole;
import com.contdistrapp.refdata.config.RefDataProperties;
import com.contdistrapp.refdata.domain.ConsistencyMode;
import com.contdistrapp.refdata.domain.EventType;
import com.contdistrapp.refdata.domain.UpdateCommand;
import com.contdistrapp.refdata.domain.UpdateItem;
import com.contdistrapp.refdata.domain.UpdateStatus;
import com.contdistrapp.refdata.error.BadRequestException;
import com.contdistrapp.refdata.error.NotFoundException;
import com.contdistrapp.refdata.persistence.PlatformRepository;
import com.contdistrapp.refdata.persistence.UpdateRequestRecord;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.util.List;
import java.util.UUID;

@Service
@ConditionalOnRefdataRole({"command-api"})
public class UpdateCommandService {

    private final RefDataProperties properties;
    private final DictionaryRegistry dictionaryRegistry;
    private final PlatformRepository repository;
    private final CommandPublisher commandPublisher;

    public UpdateCommandService(
            RefDataProperties properties,
            DictionaryRegistry dictionaryRegistry,
            PlatformRepository repository,
            CommandPublisher commandPublisher
    ) {
        this.properties = properties;
        this.dictionaryRegistry = dictionaryRegistry;
        this.repository = repository;
        this.commandPublisher = commandPublisher;
    }

    public UpdateSubmissionResponse submit(
            String tenantId,
            ConsistencyMode consistencyMode,
            Integer timeoutMs,
            UpdateCommandRequest request
    ) {
        dictionaryRegistry.required(request.dictCode());
        validateChunkedSnapshot(request);

        String eventId = request.eventId() == null || request.eventId().isBlank()
                ? UUID.randomUUID().toString()
                : request.eventId();

        List<UpdateItem> items = request.items().stream()
                .map(i -> new UpdateItem(i.key(), i.op(), i.payload()))
                .toList();

        UpdateCommand command = new UpdateCommand(
                eventId,
                tenantId,
                "REST",
                request.dictCode(),
                request.eventType(),
                request.sourceRevision(),
                request.snapshotId(),
                request.chunkIndex(),
                request.chunksTotal(),
                Instant.now(),
                items
        );

        repository.createUpdateRequestIfAbsent(command);
        commandPublisher.publish(command);

        String statusUrl = "/v1/tenants/%s/updates/%s".formatted(tenantId, eventId);

        if (consistencyMode == ConsistencyMode.ASYNC) {
            return new UpdateSubmissionResponse(eventId, UpdateStatus.PENDING, null, statusUrl);
        }

        int waitTimeout = timeoutMs == null ? properties.getConsistency().getWaitCommitTimeoutMs() : timeoutMs;
        long deadline = System.nanoTime() + waitTimeout * 1_000_000L;
        while (System.nanoTime() < deadline) {
            UpdateRequestRecord state = repository.findUpdateRequest(tenantId, eventId)
                    .orElseThrow(() -> new NotFoundException("Event not found: " + eventId));
            if (state.status() == UpdateStatus.COMMITTED) {
                return new UpdateSubmissionResponse(eventId, state.status(), state.committedVersion(), statusUrl);
            }
            if (state.status() == UpdateStatus.FAILED) {
                throw new BadRequestException("Update failed: " + state.errorMessage());
            }
            sleepQuietly(10);
        }

        return new UpdateSubmissionResponse(eventId, UpdateStatus.PENDING, null, statusUrl);
    }

    public UpdateStatusResponse status(String tenantId, String eventId) {
        UpdateRequestRecord state = repository.findUpdateRequest(tenantId, eventId)
                .orElseThrow(() -> new NotFoundException("Event not found: " + eventId));
        return new UpdateStatusResponse(
                state.eventId(),
                state.tenantId(),
                state.dictCode(),
                state.status(),
                state.committedVersion(),
                state.errorMessage()
        );
    }

    private void validateChunkedSnapshot(UpdateCommandRequest request) {
        if (request.eventType() != EventType.SNAPSHOT) {
            return;
        }

        boolean anyChunkField = request.snapshotId() != null || request.chunkIndex() != null || request.chunksTotal() != null;
        boolean allChunkFields = request.snapshotId() != null && request.chunkIndex() != null && request.chunksTotal() != null;

        if (!anyChunkField) {
            return;
        }

        if (!allChunkFields) {
            throw new BadRequestException("snapshotId/chunkIndex/chunksTotal are required together for chunked snapshot");
        }

        if (request.chunksTotal() < 1 || request.chunkIndex() < 1 || request.chunkIndex() > request.chunksTotal()) {
            throw new BadRequestException("Invalid chunk metadata");
        }
    }

    private void sleepQuietly(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
        }
    }
}
