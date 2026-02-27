package com.contdistrapp.refdata.persistence;

import com.contdistrapp.refdata.domain.UpdateCommand;
import com.contdistrapp.refdata.domain.UpdateItem;
import com.fasterxml.jackson.databind.JsonNode;

import java.util.List;
import java.util.Map;

public interface DictionaryProvider {

    Map<String, JsonNode> loadAll(String tenantId, String dictCode);

    long getCommittedVersion(String tenantId, String dictCode);

    void applyDelta(String tenantId, String dictCode, List<UpdateItem> items, UpdateCommand event, long eventVersion);

    void applySnapshot(String tenantId, String dictCode, List<UpdateItem> items, UpdateCommand event, long eventVersion);
}
