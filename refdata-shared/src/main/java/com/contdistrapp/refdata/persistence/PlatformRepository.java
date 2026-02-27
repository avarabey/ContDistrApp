package com.contdistrapp.refdata.persistence;

import com.contdistrapp.refdata.domain.EventType;
import com.contdistrapp.refdata.domain.UpdateCommand;
import com.contdistrapp.refdata.domain.UpdateItem;
import com.contdistrapp.refdata.domain.UpdateStatus;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.dao.DataAccessException;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.stereotype.Repository;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

@Repository
public class PlatformRepository {

    private static final TypeReference<List<UpdateItem>> ITEMS_TYPE = new TypeReference<>() {
    };

    private final NamedParameterJdbcTemplate jdbc;
    private final ObjectMapper objectMapper;
    private final boolean postgresDialect;

    public PlatformRepository(NamedParameterJdbcTemplate jdbc, ObjectMapper objectMapper) {
        this.jdbc = jdbc;
        this.objectMapper = objectMapper;
        this.postgresDialect = detectPostgresDialect(jdbc.getJdbcTemplate().getDataSource());
    }

    public void createUpdateRequestIfAbsent(UpdateCommand command) {
        MapSqlParameterSource params = new MapSqlParameterSource()
                .addValue("tenantId", command.tenantId())
                .addValue("eventId", command.eventId())
                .addValue("dictCode", command.dictCode())
                .addValue("status", UpdateStatus.PENDING.name())
                .addValue("eventType", command.eventType().name())
                .addValue("snapshotId", command.snapshotId())
                .addValue("now", dbNow());
        if (postgresDialect) {
            jdbc.update("""
                    insert into update_request(
                        tenant_id, event_id, dict_code, status, event_type, snapshot_id,
                        created_at, updated_at
                    ) values (
                        :tenantId, :eventId, :dictCode, :status, :eventType, :snapshotId,
                        :now, :now
                    )
                    on conflict (tenant_id, event_id) do nothing
                    """, params);
            return;
        }
        try {
            jdbc.update("""
                    insert into update_request(
                        tenant_id, event_id, dict_code, status, event_type, snapshot_id,
                        created_at, updated_at
                    ) values (
                        :tenantId, :eventId, :dictCode, :status, :eventType, :snapshotId,
                        :now, :now
                    )
                    """, params);
        } catch (DuplicateKeyException ignored) {
            // idempotent create (H2 path)
        }
    }

    public Optional<UpdateRequestRecord> findUpdateRequest(String tenantId, String eventId) {
        MapSqlParameterSource params = new MapSqlParameterSource()
                .addValue("tenantId", tenantId)
                .addValue("eventId", eventId);
        List<UpdateRequestRecord> rows = jdbc.query("""
                select tenant_id, event_id, dict_code, status, committed_version, error_message
                from update_request
                where tenant_id = :tenantId and event_id = :eventId
                """, params, (rs, rowNum) -> new UpdateRequestRecord(
                rs.getString("tenant_id"),
                rs.getString("event_id"),
                rs.getString("dict_code"),
                UpdateStatus.valueOf(rs.getString("status")),
                (Long) rs.getObject("committed_version"),
                rs.getString("error_message")
        ));
        return rows.stream().findFirst();
    }

    public void markUpdateCommitted(String tenantId, String eventId, long version) {
        MapSqlParameterSource params = new MapSqlParameterSource()
                .addValue("tenantId", tenantId)
                .addValue("eventId", eventId)
                .addValue("status", UpdateStatus.COMMITTED.name())
                .addValue("version", version)
                .addValue("now", dbNow());
        jdbc.update("""
                update update_request
                set status = :status,
                    committed_version = :version,
                    error_message = null,
                    updated_at = :now
                where tenant_id = :tenantId
                  and event_id = :eventId
                """, params);
    }

    public void markSnapshotCommitted(String tenantId, String dictCode, String snapshotId, long version) {
        MapSqlParameterSource params = new MapSqlParameterSource()
                .addValue("tenantId", tenantId)
                .addValue("dictCode", dictCode)
                .addValue("snapshotId", snapshotId)
                .addValue("status", UpdateStatus.COMMITTED.name())
                .addValue("version", version)
                .addValue("now", dbNow());
        jdbc.update("""
                update update_request
                set status = :status,
                    committed_version = :version,
                    error_message = null,
                    updated_at = :now
                where tenant_id = :tenantId
                  and dict_code = :dictCode
                  and snapshot_id = :snapshotId
                  and status = 'PENDING'
                """, params);
    }

    public void markUpdateFailed(String tenantId, String eventId, String message) {
        MapSqlParameterSource params = new MapSqlParameterSource()
                .addValue("tenantId", tenantId)
                .addValue("eventId", eventId)
                .addValue("status", UpdateStatus.FAILED.name())
                .addValue("message", message)
                .addValue("now", dbNow());
        jdbc.update("""
                update update_request
                set status = :status,
                    error_message = :message,
                    updated_at = :now
                where tenant_id = :tenantId
                  and event_id = :eventId
                """, params);
    }

    public boolean tryMarkProcessed(UpdateCommand command) {
        MapSqlParameterSource params = new MapSqlParameterSource()
                .addValue("tenantId", command.tenantId())
                .addValue("eventId", command.eventId())
                .addValue("source", command.source())
                .addValue("processedAt", dbNow());
        if (postgresDialect) {
            int updated = jdbc.update("""
                    insert into processed_event(tenant_id, event_id, source, processed_at)
                    values (:tenantId, :eventId, :source, :processedAt)
                    on conflict (tenant_id, event_id) do nothing
                    """, params);
            return updated == 1;
        }
        try {
            int updated = jdbc.update("""
                    insert into processed_event(tenant_id, event_id, source, processed_at)
                    values (:tenantId, :eventId, :source, :processedAt)
                    """, params);
            return updated == 1;
        } catch (DuplicateKeyException ex) {
            return false;
        }
    }

    public DictionaryMetaRecord dictionaryMeta(String tenantId, String dictCode) {
        MapSqlParameterSource params = new MapSqlParameterSource()
                .addValue("tenantId", tenantId)
                .addValue("dictCode", dictCode);
        List<DictionaryMetaRecord> rows = jdbc.query("""
                select version, last_source_revision
                from dictionary_meta
                where tenant_id = :tenantId
                  and dict_code = :dictCode
                """, params, (rs, rowNum) -> new DictionaryMetaRecord(
                rs.getLong("version"),
                (Long) rs.getObject("last_source_revision")
        ));
        return rows.isEmpty() ? new DictionaryMetaRecord(0, null) : rows.getFirst();
    }

    public long allocateNextVersion(String tenantId, String dictCode, Long sourceRevision) {
        for (int attempt = 0; attempt < 3; attempt++) {
            MapSqlParameterSource updateParams = new MapSqlParameterSource()
                    .addValue("tenantId", tenantId)
                    .addValue("dictCode", dictCode)
                    .addValue("sourceRevision", sourceRevision)
                    .addValue("now", dbNow());

            int updated = jdbc.update("""
                    update dictionary_meta
                    set version = version + 1,
                        last_source_revision = case
                            when :sourceRevision is null then last_source_revision
                            when last_source_revision is null then :sourceRevision
                            when :sourceRevision > last_source_revision then :sourceRevision
                            else last_source_revision
                        end,
                        updated_at = :now
                    where tenant_id = :tenantId
                      and dict_code = :dictCode
                    """, updateParams);

            if (updated == 1) {
                return dictionaryMeta(tenantId, dictCode).version();
            }

            MapSqlParameterSource insertParams = new MapSqlParameterSource()
                    .addValue("tenantId", tenantId)
                    .addValue("dictCode", dictCode)
                    .addValue("version", 1L)
                    .addValue("sourceRevision", sourceRevision)
                    .addValue("now", dbNow());
            if (postgresDialect) {
                int inserted = jdbc.update("""
                        insert into dictionary_meta(
                            tenant_id, dict_code, version, last_source_revision, updated_at
                        ) values (
                            :tenantId, :dictCode, :version, :sourceRevision, :now
                        )
                        on conflict (tenant_id, dict_code) do nothing
                        """, insertParams);
                if (inserted == 1) {
                    return 1L;
                }
                continue;
            }
            try {
                jdbc.update("""
                        insert into dictionary_meta(
                            tenant_id, dict_code, version, last_source_revision, updated_at
                        ) values (
                            :tenantId, :dictCode, :version, :sourceRevision, :now
                        )
                        """, insertParams);
                return 1L;
            } catch (DuplicateKeyException ignored) {
                // lost race, retry (H2 path)
            }
        }
        throw new DataAccessException("Unable to allocate next version") {
        };
    }

    public void insertOutboxEvent(String tenantId, String eventId, String dictCode, long version, String payload) {
        MapSqlParameterSource params = new MapSqlParameterSource()
                .addValue("tenantId", tenantId)
                .addValue("eventId", eventId)
                .addValue("dictCode", dictCode)
                .addValue("version", version)
                .addValue("payload", payload)
                .addValue("createdAt", dbNow());
        jdbc.update("""
                insert into outbox_event(tenant_id, event_id, dict_code, version, payload, created_at, published)
                values (:tenantId, :eventId, :dictCode, :version, :payload, :createdAt, false)
                """, params);
    }

    public List<OutboxEventRecord> fetchUnpublishedOutbox(int limit) {
        MapSqlParameterSource params = new MapSqlParameterSource().addValue("limit", limit);
        return jdbc.query("""
                select id, tenant_id, event_id, dict_code, version, payload
                from outbox_event
                where published = false
                order by id asc
                limit :limit
                """, params, (rs, rowNum) -> new OutboxEventRecord(
                rs.getLong("id"),
                rs.getString("tenant_id"),
                rs.getString("event_id"),
                rs.getString("dict_code"),
                rs.getLong("version"),
                rs.getString("payload")
        ));
    }

    public void markOutboxPublished(long id) {
        MapSqlParameterSource params = new MapSqlParameterSource()
                .addValue("id", id)
                .addValue("publishedAt", dbNow());
        jdbc.update("""
                update outbox_event
                set published = true,
                    published_at = :publishedAt
                where id = :id
                """, params);
    }

    public void saveSnapshotChunk(UpdateCommand command) {
        if (command.snapshotId() == null || command.chunkIndex() == null || command.chunksTotal() == null) {
            throw new IllegalArgumentException("Snapshot chunk metadata is required");
        }
        String payload;
        try {
            payload = objectMapper.writeValueAsString(command.items());
        } catch (JsonProcessingException e) {
            throw new IllegalArgumentException("Unable to serialize snapshot chunk", e);
        }

        MapSqlParameterSource deleteParams = new MapSqlParameterSource()
                .addValue("tenantId", command.tenantId())
                .addValue("dictCode", command.dictCode())
                .addValue("snapshotId", command.snapshotId())
                .addValue("chunkIndex", command.chunkIndex());
        jdbc.update("""
                delete from snapshot_chunk
                where tenant_id = :tenantId
                  and dict_code = :dictCode
                  and snapshot_id = :snapshotId
                  and chunk_index = :chunkIndex
                """, deleteParams);

        MapSqlParameterSource insertParams = new MapSqlParameterSource()
                .addValue("tenantId", command.tenantId())
                .addValue("dictCode", command.dictCode())
                .addValue("snapshotId", command.snapshotId())
                .addValue("chunkIndex", command.chunkIndex())
                .addValue("chunksTotal", command.chunksTotal())
                .addValue("itemsPayload", payload)
                .addValue("createdAt", dbNow());
        jdbc.update("""
                insert into snapshot_chunk(
                    tenant_id, dict_code, snapshot_id, chunk_index, chunks_total, items_payload, created_at
                ) values (
                    :tenantId, :dictCode, :snapshotId, :chunkIndex, :chunksTotal, :itemsPayload, :createdAt
                )
                """, insertParams);
    }

    public int countSnapshotChunks(String tenantId, String dictCode, String snapshotId) {
        MapSqlParameterSource params = new MapSqlParameterSource()
                .addValue("tenantId", tenantId)
                .addValue("dictCode", dictCode)
                .addValue("snapshotId", snapshotId);
        Integer count = jdbc.queryForObject("""
                select count(*)
                from snapshot_chunk
                where tenant_id = :tenantId
                  and dict_code = :dictCode
                  and snapshot_id = :snapshotId
                """, params, Integer.class);
        return count == null ? 0 : count;
    }

    public List<UpdateItem> loadSnapshotItems(String tenantId, String dictCode, String snapshotId) {
        MapSqlParameterSource params = new MapSqlParameterSource()
                .addValue("tenantId", tenantId)
                .addValue("dictCode", dictCode)
                .addValue("snapshotId", snapshotId);
        List<String> chunks = jdbc.query("""
                select items_payload
                from snapshot_chunk
                where tenant_id = :tenantId
                  and dict_code = :dictCode
                  and snapshot_id = :snapshotId
                order by chunk_index asc
                """, params, (rs, rowNum) -> rs.getString("items_payload"));

        List<UpdateItem> items = new ArrayList<>();
        for (String chunk : chunks) {
            try {
                items.addAll(objectMapper.readValue(chunk, ITEMS_TYPE));
            } catch (JsonProcessingException e) {
                throw new IllegalArgumentException("Unable to deserialize snapshot chunk", e);
            }
        }
        return items;
    }

    public void clearSnapshotChunks(String tenantId, String dictCode, String snapshotId) {
        MapSqlParameterSource params = new MapSqlParameterSource()
                .addValue("tenantId", tenantId)
                .addValue("dictCode", dictCode)
                .addValue("snapshotId", snapshotId);
        jdbc.update("""
                delete from snapshot_chunk
                where tenant_id = :tenantId
                  and dict_code = :dictCode
                  and snapshot_id = :snapshotId
                """, params);
    }

    public long currentCommittedVersion(String tenantId, String dictCode) {
        return dictionaryMeta(tenantId, dictCode).version();
    }

    public Optional<Long> lastSourceRevision(String tenantId, String dictCode) {
        return Optional.ofNullable(dictionaryMeta(tenantId, dictCode).lastSourceRevision());
    }

    public boolean isSnapshotEvent(String tenantId, String eventId) {
        MapSqlParameterSource params = new MapSqlParameterSource()
                .addValue("tenantId", tenantId)
                .addValue("eventId", eventId);
        List<String> rows = jdbc.query("""
                select event_type
                from update_request
                where tenant_id = :tenantId
                  and event_id = :eventId
                """, params, (rs, rowNum) -> rs.getString("event_type"));
        return rows.stream().findFirst().map(v -> EventType.SNAPSHOT.name().equals(v)).orElse(false);
    }

    private Timestamp dbNow() {
        return Timestamp.from(Instant.now());
    }

    private boolean detectPostgresDialect(DataSource dataSource) {
        if (dataSource == null) {
            return false;
        }
        try (Connection connection = dataSource.getConnection()) {
            String product = connection.getMetaData().getDatabaseProductName();
            return product != null && product.toLowerCase().contains("postgresql");
        } catch (SQLException ignored) {
            return false;
        }
    }
}
