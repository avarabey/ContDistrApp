package com.contdistrapp.refdata.service;

import com.contdistrapp.refdata.bus.InvalidationBus;
import com.contdistrapp.refdata.config.role.ConditionalOnRefdataRole;
import com.contdistrapp.refdata.domain.DataSourceType;
import com.contdistrapp.refdata.domain.InvalidationEvent;
import com.contdistrapp.refdata.error.NotFoundException;
import com.contdistrapp.refdata.error.VersionNotCommittedException;
import com.contdistrapp.refdata.persistence.DictionaryProvider;
import com.fasterxml.jackson.databind.JsonNode;
import jakarta.annotation.PreDestroy;
import org.springframework.stereotype.Service;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantLock;

@Service
@ConditionalOnRefdataRole({"query-api"})
public class QueryService {

    private final DictionaryProvider dictionaryProvider;
    private final DictionaryRegistry dictionaryRegistry;
    private final RefDataTimeouts timeouts;
    private final ExecutorService cacheReloadExecutor;

    private final Map<CacheKey, CacheBucket> cache = new ConcurrentHashMap<>();

    public QueryService(
            DictionaryProvider dictionaryProvider,
            DictionaryRegistry dictionaryRegistry,
            RefDataTimeouts timeouts,
            ExecutorService cacheReloadExecutor,
            InvalidationBus invalidationBus
    ) {
        this.dictionaryProvider = dictionaryProvider;
        this.dictionaryRegistry = dictionaryRegistry;
        this.timeouts = timeouts;
        this.cacheReloadExecutor = cacheReloadExecutor;
        invalidationBus.subscribe(this::onInvalidation);
    }

    public QueryReadResult readItem(String tenantId, String dictCode, String key, Long minVersion) {
        QueryReadResult full = readAll(tenantId, dictCode, minVersion);
        JsonNode value = full.items().get(key);
        if (value == null) {
            throw new NotFoundException("Item not found: " + key);
        }
        return new QueryReadResult(full.version(), full.sourceType(), Collections.singletonMap(key, value));
    }

    public QueryReadResult readItems(String tenantId, String dictCode, List<String> keys, Long minVersion) {
        QueryReadResult full = readAll(tenantId, dictCode, minVersion);
        Map<String, JsonNode> selected = new LinkedHashMap<>();
        for (String key : keys) {
            JsonNode value = full.items().get(key);
            if (value != null) {
                selected.put(key, value);
            }
        }
        return new QueryReadResult(full.version(), full.sourceType(), selected);
    }

    public QueryReadResult readAll(String tenantId, String dictCode, Long minVersion) {
        dictionaryRegistry.required(dictCode);
        CacheKey key = new CacheKey(tenantId, dictCode);
        CacheBucket bucket = cache.computeIfAbsent(key, ignored -> new CacheBucket());

        ensureLoaded(key, bucket);
        CacheSnapshot current = bucket.snapshotRef.get();

        if (minVersion == null || current.version() >= minVersion) {
            return new QueryReadResult(current.version(), DataSourceType.MEMORY, current.items());
        }

        waitForVersion(key, bucket, minVersion);
        current = bucket.snapshotRef.get();
        if (current.version() >= minVersion) {
            return new QueryReadResult(current.version(), DataSourceType.MEMORY, current.items());
        }

        long committedVersion = dictionaryProvider.getCommittedVersion(tenantId, dictCode);
        if (committedVersion >= minVersion) {
            Map<String, JsonNode> fallback = Map.copyOf(dictionaryProvider.loadAll(tenantId, dictCode));
            refreshAsync(key, committedVersion);
            return new QueryReadResult(committedVersion, DataSourceType.POSTGRES_FALLBACK, fallback);
        }

        throw new VersionNotCommittedException(minVersion, committedVersion);
    }

    public long currentVersion(String tenantId, String dictCode) {
        dictionaryRegistry.required(dictCode);
        CacheKey key = new CacheKey(tenantId, dictCode);
        CacheBucket bucket = cache.computeIfAbsent(key, ignored -> new CacheBucket());
        ensureLoaded(key, bucket);
        return bucket.snapshotRef.get().version();
    }

    private void onInvalidation(InvalidationEvent event) {
        CacheKey key = new CacheKey(event.tenantId(), event.dictCode());
        refreshAsync(key, event.version());
    }

    private void refreshAsync(CacheKey key, long targetVersion) {
        cacheReloadExecutor.submit(() -> {
            CacheBucket bucket = cache.computeIfAbsent(key, ignored -> new CacheBucket());
            reloadIfNeeded(key, bucket, targetVersion);
        });
    }

    private void ensureLoaded(CacheKey key, CacheBucket bucket) {
        if (bucket.snapshotRef.get().version() > 0) {
            return;
        }
        reloadIfNeeded(key, bucket, 1);
    }

    private void waitForVersion(CacheKey key, CacheBucket bucket, long minVersion) {
        long deadline = System.nanoTime() + timeouts.waitForReloadMs() * 1_000_000L;
        while (System.nanoTime() < deadline) {
            if (bucket.snapshotRef.get().version() >= minVersion) {
                return;
            }
            reloadIfNeeded(key, bucket, minVersion);
            if (bucket.snapshotRef.get().version() >= minVersion) {
                return;
            }
            sleepQuietly(5);
        }
    }

    private void reloadIfNeeded(CacheKey key, CacheBucket bucket, long targetVersion) {
        if (bucket.snapshotRef.get().version() >= targetVersion) {
            return;
        }

        if (!bucket.reloadLock.tryLock()) {
            return;
        }

        try {
            CacheSnapshot current = bucket.snapshotRef.get();
            if (current.version() >= targetVersion) {
                return;
            }

            long committedVersion = dictionaryProvider.getCommittedVersion(key.tenantId(), key.dictCode());
            if (committedVersion <= current.version()) {
                return;
            }

            Map<String, JsonNode> loaded = Map.copyOf(dictionaryProvider.loadAll(key.tenantId(), key.dictCode()));
            bucket.snapshotRef.set(new CacheSnapshot(committedVersion, loaded));
        } finally {
            bucket.reloadLock.unlock();
        }
    }

    private void sleepQuietly(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
        }
    }

    @PreDestroy
    public void shutdown() {
        cacheReloadExecutor.shutdown();
    }

    private record CacheKey(String tenantId, String dictCode) {
        private CacheKey {
            Objects.requireNonNull(tenantId, "tenantId");
            Objects.requireNonNull(dictCode, "dictCode");
        }
    }

    private static final class CacheBucket {
        private final AtomicReference<CacheSnapshot> snapshotRef =
                new AtomicReference<>(new CacheSnapshot(0, Map.of()));
        private final ReentrantLock reloadLock = new ReentrantLock();
    }

    private record CacheSnapshot(long version, Map<String, JsonNode> items) {
    }
}
