package com.contdistrapp.refdata.persistence;

import com.contdistrapp.refdata.config.RefDataProperties;
import com.contdistrapp.refdata.domain.ItemOperation;
import com.contdistrapp.refdata.domain.UpdateCommand;
import com.contdistrapp.refdata.domain.UpdateItem;
import com.contdistrapp.refdata.service.DictionaryRegistry;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.stereotype.Component;

import java.time.Instant;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

@Component
public class PostgresSqlProvider implements DictionaryProvider {

    private final NamedParameterJdbcTemplate jdbc;
    private final PlatformRepository repository;
    private final DictionaryRegistry dictionaryRegistry;
    private final ObjectMapper objectMapper;

    public PostgresSqlProvider(
            NamedParameterJdbcTemplate jdbc,
            PlatformRepository repository,
            DictionaryRegistry dictionaryRegistry,
            ObjectMapper objectMapper
    ) {
        this.jdbc = jdbc;
        this.repository = repository;
        this.dictionaryRegistry = dictionaryRegistry;
        this.objectMapper = objectMapper;
    }

    @Override
    public Map<String, JsonNode> loadAll(String tenantId, String dictCode) {
        RefDataProperties.Dictionary cfg = dictionaryRegistry.required(dictCode);

        MapSqlParameterSource params = new MapSqlParameterSource()
                .addValue("tenantId", tenantId)
                .addValue("dictCode", cfg.getCode());

        Map<String, JsonNode> result = new LinkedHashMap<>();
        jdbc.query(cfg.getLoadSql(), params, rs -> {
            String key = rs.getString("k");
            String raw = rs.getString("v");
            if (key == null) {
                return;
            }
            if (raw == null || raw.isBlank()) {
                result.put(key, objectMapper.nullNode());
                return;
            }
            try {
                result.put(key, objectMapper.readTree(raw));
            } catch (JsonProcessingException e) {
                throw new IllegalArgumentException("loadSql must return JSON in column v", e);
            }
        });
        return result;
    }

    @Override
    public long getCommittedVersion(String tenantId, String dictCode) {
        return repository.currentCommittedVersion(tenantId, dictCode);
    }

    @Override
    public void applyDelta(String tenantId, String dictCode, List<UpdateItem> items, UpdateCommand event, long eventVersion) {
        RefDataProperties.Dictionary cfg = dictionaryRegistry.required(dictCode);
        RefDataProperties.Apply apply = cfg.getApply();

        boolean hasTemplates = apply != null && apply.getUpsertSql() != null && apply.getDeleteSql() != null;
        for (UpdateItem item : items) {
            if (item.op() == ItemOperation.UPSERT) {
                if (hasTemplates) {
                    executeTemplate(apply.getUpsertSql(), tenantId, cfg.getCode(), item, event, eventVersion);
                } else {
                    genericUpsert(tenantId, cfg.getCode(), item);
                }
            } else if (item.op() == ItemOperation.DELETE) {
                if (hasTemplates) {
                    executeTemplate(apply.getDeleteSql(), tenantId, cfg.getCode(), item, event, eventVersion);
                } else {
                    genericDelete(tenantId, cfg.getCode(), item.key());
                }
            }
        }
    }

    @Override
    public void applySnapshot(String tenantId, String dictCode, List<UpdateItem> items, UpdateCommand event, long eventVersion) {
        RefDataProperties.Dictionary cfg = dictionaryRegistry.required(dictCode);
        RefDataProperties.Apply apply = cfg.getApply();

        if (apply != null && apply.getSnapshotReplaceSql() != null && !apply.getSnapshotReplaceSql().isBlank()) {
            MapSqlParameterSource params = baseParams(tenantId, cfg.getCode(), event, eventVersion)
                    .addValue("snapshotJson", serialize(items));
            jdbc.update(apply.getSnapshotReplaceSql(), params);
            return;
        }

        // Generic FULL_REPLACE fallback for platform table.
        MapSqlParameterSource deleteParams = new MapSqlParameterSource()
                .addValue("tenantId", tenantId)
                .addValue("dictCode", cfg.getCode());
        jdbc.update("""
                delete from dictionary_item
                where tenant_id = :tenantId
                  and dict_code = :dictCode
                """, deleteParams);

        for (UpdateItem item : items) {
            if (item.op() == ItemOperation.DELETE) {
                continue;
            }
            genericUpsert(tenantId, cfg.getCode(), item);
        }
    }

    private void executeTemplate(
            String sql,
            String tenantId,
            String dictCode,
            UpdateItem item,
            UpdateCommand event,
            long eventVersion
    ) {
        MapSqlParameterSource params = baseParams(tenantId, dictCode, event, eventVersion)
                .addValue("key", item.key())
                .addValue("payload", serialize(item.payload()));
        jdbc.update(sql, params);
    }

    private MapSqlParameterSource baseParams(String tenantId, String dictCode, UpdateCommand event, long eventVersion) {
        return new MapSqlParameterSource()
                .addValue("tenantId", tenantId)
                .addValue("dictCode", dictCode)
                .addValue("eventId", event.eventId())
                .addValue("eventVersion", eventVersion)
                .addValue("eventEpoch", event.occurredAt().toEpochMilli())
                .addValue("sourceRevision", event.sourceRevision())
                .addValue("snapshotId", event.snapshotId())
                .addValue("occurredAt", event.occurredAt())
                .addValue("now", Instant.now());
    }

    private void genericUpsert(String tenantId, String dictCode, UpdateItem item) {
        MapSqlParameterSource params = new MapSqlParameterSource()
                .addValue("tenantId", tenantId)
                .addValue("dictCode", dictCode)
                .addValue("key", item.key())
                .addValue("payload", serialize(item.payload()));
        int updated = jdbc.update("""
                update dictionary_item
                set payload = :payload,
                    deleted = false,
                    updated_at = CURRENT_TIMESTAMP
                where tenant_id = :tenantId
                  and dict_code = :dictCode
                  and item_key = :key
                """, params);

        if (updated > 0) {
            return;
        }

        try {
            jdbc.update("""
                    insert into dictionary_item(tenant_id, dict_code, item_key, payload, deleted, updated_at)
                    values (:tenantId, :dictCode, :key, :payload, false, CURRENT_TIMESTAMP)
                    """, params);
        } catch (DuplicateKeyException race) {
            jdbc.update("""
                    update dictionary_item
                    set payload = :payload,
                        deleted = false,
                        updated_at = CURRENT_TIMESTAMP
                    where tenant_id = :tenantId
                      and dict_code = :dictCode
                      and item_key = :key
                    """, params);
        }
    }

    private void genericDelete(String tenantId, String dictCode, String key) {
        MapSqlParameterSource params = new MapSqlParameterSource()
                .addValue("tenantId", tenantId)
                .addValue("dictCode", dictCode)
                .addValue("key", key);
        jdbc.update("""
                update dictionary_item
                set deleted = true,
                    updated_at = CURRENT_TIMESTAMP
                where tenant_id = :tenantId
                  and dict_code = :dictCode
                  and item_key = :key
                """, params);
    }

    private String serialize(Object value) {
        try {
            return objectMapper.writeValueAsString(value);
        } catch (JsonProcessingException e) {
            throw new IllegalArgumentException("Unable to serialize JSON payload", e);
        }
    }
}
