package com.contdistrapp.refdata.api;

import com.fasterxml.jackson.databind.JsonNode;

public record DictionaryItemResponse(String key, JsonNode payload) {
}
