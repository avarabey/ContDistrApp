package com.contdistrapp.refdata.domain;

import java.time.Instant;

public record InvalidationEvent(
        String eventId,
        String tenantId,
        String dictCode,
        long version,
        Instant committedAt
) {
}
