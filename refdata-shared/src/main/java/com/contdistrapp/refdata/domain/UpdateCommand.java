package com.contdistrapp.refdata.domain;

import java.time.Instant;
import java.util.List;

public record UpdateCommand(
        String eventId,
        String tenantId,
        String source,
        String dictCode,
        EventType eventType,
        Long sourceRevision,
        String snapshotId,
        Integer chunkIndex,
        Integer chunksTotal,
        Instant occurredAt,
        List<UpdateItem> items
) {
    public String partitionKey() {
        return tenantId + ":" + dictCode;
    }

    public boolean isChunkedSnapshot() {
        return eventType == EventType.SNAPSHOT && snapshotId != null && chunkIndex != null && chunksTotal != null && chunksTotal > 1;
    }
}
