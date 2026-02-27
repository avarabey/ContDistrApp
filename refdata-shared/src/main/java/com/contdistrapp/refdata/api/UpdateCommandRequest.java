package com.contdistrapp.refdata.api;

import com.contdistrapp.refdata.domain.EventType;
import jakarta.validation.Valid;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotEmpty;
import jakarta.validation.constraints.NotNull;

import java.util.List;

public record UpdateCommandRequest(
        String eventId,
        @NotBlank String dictCode,
        @NotNull EventType eventType,
        Long sourceRevision,
        String snapshotId,
        Integer chunkIndex,
        Integer chunksTotal,
        @NotEmpty List<@Valid UpdateItemRequest> items
) {
}
