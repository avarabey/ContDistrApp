package com.contdistrapp.refdata.persistence;

import com.contdistrapp.refdata.domain.UpdateStatus;

public record UpdateRequestRecord(
        String tenantId,
        String eventId,
        String dictCode,
        UpdateStatus status,
        Long committedVersion,
        String errorMessage
) {
}
