package com.contdistrapp.refdata.service;

import com.contdistrapp.refdata.domain.DataSourceType;
import com.fasterxml.jackson.databind.JsonNode;

import java.util.Map;

public record QueryReadResult(long version, DataSourceType sourceType, Map<String, JsonNode> items) {
}
