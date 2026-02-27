package com.contdistrapp.refdata.api;

import com.contdistrapp.refdata.domain.UpdateStatus;

public record UpdateStatusResponse(
        String eventId,
        String tenantId,
        String dictCode,
        UpdateStatus status,
        Long committedVersion,
        String errorMessage
) {
}
