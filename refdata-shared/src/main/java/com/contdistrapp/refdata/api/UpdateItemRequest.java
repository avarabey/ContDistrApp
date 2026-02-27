package com.contdistrapp.refdata.api;

import com.contdistrapp.refdata.domain.ItemOperation;
import com.fasterxml.jackson.databind.JsonNode;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;

public record UpdateItemRequest(
        @NotBlank String key,
        @NotNull ItemOperation op,
        JsonNode payload
) {
}
