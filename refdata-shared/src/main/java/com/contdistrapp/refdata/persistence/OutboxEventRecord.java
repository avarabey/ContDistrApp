package com.contdistrapp.refdata.persistence;

public record OutboxEventRecord(
        long id,
        String tenantId,
        String eventId,
        String dictCode,
        long version,
        String payload
) {
}
