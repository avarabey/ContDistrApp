package com.contdistrapp.refdata.api;

import com.fasterxml.jackson.databind.JsonNode;

import java.util.Map;

public record DictionaryItemsResponse(Map<String, JsonNode> items) {
}
