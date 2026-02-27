package com.contdistrapp.refdata.domain;

import com.fasterxml.jackson.databind.JsonNode;

public record UpdateItem(String key, ItemOperation op, JsonNode payload) {
}
