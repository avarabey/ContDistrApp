package com.contdistrapp.refdata.api;

import com.contdistrapp.refdata.domain.UpdateStatus;

public record UpdateSubmissionResponse(
        String eventId,
        UpdateStatus status,
        Long committedVersion,
        String statusUrl
) {
}
