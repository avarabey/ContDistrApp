package com.contdistrapp.refdata.config;

import jakarta.validation.Valid;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.validation.annotation.Validated;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;

@Validated
@ConfigurationProperties(prefix = "refdata")
public class RefDataProperties {

    @Valid
    private Consistency consistency = new Consistency();

    @Valid
    private Query query = new Query();

    @Valid
    private Outbox outbox = new Outbox();

    @Valid
    private Cache cache = new Cache();

    @Valid
    private Kafka kafka = new Kafka();

    @Valid
    private Redis redis = new Redis();

    @Valid
    private List<Dictionary> dictionaries = new ArrayList<>();

    public Consistency getConsistency() {
        return consistency;
    }

    public void setConsistency(Consistency consistency) {
        this.consistency = consistency;
    }

    public Query getQuery() {
        return query;
    }

    public void setQuery(Query query) {
        this.query = query;
    }

    public Outbox getOutbox() {
        return outbox;
    }

    public void setOutbox(Outbox outbox) {
        this.outbox = outbox;
    }

    public Cache getCache() {
        return cache;
    }

    public void setCache(Cache cache) {
        this.cache = cache;
    }

    public List<Dictionary> getDictionaries() {
        return dictionaries;
    }

    public void setDictionaries(List<Dictionary> dictionaries) {
        this.dictionaries = dictionaries;
    }

    public Kafka getKafka() {
        return kafka;
    }

    public void setKafka(Kafka kafka) {
        this.kafka = kafka;
    }

    public Redis getRedis() {
        return redis;
    }

    public void setRedis(Redis redis) {
        this.redis = redis;
    }

    public Optional<Dictionary> findDictionary(String dictCode) {
        if (dictCode == null) {
            return Optional.empty();
        }
        return dictionaries.stream()
                .filter(Dictionary::isEnabled)
                .filter(d -> d.getCode().equalsIgnoreCase(dictCode))
                .findFirst();
    }

    public Map<String, Dictionary> dictionaryMap() {
        return dictionaries.stream()
                .filter(Dictionary::isEnabled)
                .collect(Collectors.toUnmodifiableMap(
                        d -> d.getCode().toUpperCase(Locale.ROOT),
                        Function.identity(),
                        (a, b) -> b
                ));
    }

    public static class Consistency {

        @Min(10)
        private int waitCommitTimeoutMs = 300;

        public int getWaitCommitTimeoutMs() {
            return waitCommitTimeoutMs;
        }

        public void setWaitCommitTimeoutMs(int waitCommitTimeoutMs) {
            this.waitCommitTimeoutMs = waitCommitTimeoutMs;
        }
    }

    public static class Query {

        @Min(1)
        private int waitForReloadMs = 100;

        public int getWaitForReloadMs() {
            return waitForReloadMs;
        }

        public void setWaitForReloadMs(int waitForReloadMs) {
            this.waitForReloadMs = waitForReloadMs;
        }
    }

    public static class Outbox {

        @Min(1)
        private int pollIntervalMs = 50;

        @Min(1)
        private int batchSize = 200;

        public int getPollIntervalMs() {
            return pollIntervalMs;
        }

        public void setPollIntervalMs(int pollIntervalMs) {
            this.pollIntervalMs = pollIntervalMs;
        }

        public int getBatchSize() {
            return batchSize;
        }

        public void setBatchSize(int batchSize) {
            this.batchSize = batchSize;
        }
    }

    public static class Cache {

        @Min(1)
        private int reloadParallelism = 8;

        public int getReloadParallelism() {
            return reloadParallelism;
        }

        public void setReloadParallelism(int reloadParallelism) {
            this.reloadParallelism = reloadParallelism;
        }
    }

    public static class Kafka {

        private boolean enabled = false;
        private boolean externalEnabled = false;
        private String commandsTopic = "refdata.commands";
        private String externalTopic = "";
        private String groupId = "refdata-apply-service";

        public boolean isEnabled() {
            return enabled;
        }

        public void setEnabled(boolean enabled) {
            this.enabled = enabled;
        }

        public boolean isExternalEnabled() {
            return externalEnabled;
        }

        public void setExternalEnabled(boolean externalEnabled) {
            this.externalEnabled = externalEnabled;
        }

        public String getCommandsTopic() {
            return commandsTopic;
        }

        public void setCommandsTopic(String commandsTopic) {
            this.commandsTopic = commandsTopic;
        }

        public String getExternalTopic() {
            return externalTopic;
        }

        public void setExternalTopic(String externalTopic) {
            this.externalTopic = externalTopic;
        }

        public String getGroupId() {
            return groupId;
        }

        public void setGroupId(String groupId) {
            this.groupId = groupId;
        }
    }

    public static class Redis {

        private boolean enabled = false;
        private String pubChannel = "refdata:inv:pub";
        private String streamKey = "refdata:inv:stream";

        @Min(100)
        private int streamRecoveryPollMs = 1000;

        private String streamStartId = "0-0";

        public boolean isEnabled() {
            return enabled;
        }

        public void setEnabled(boolean enabled) {
            this.enabled = enabled;
        }

        public String getPubChannel() {
            return pubChannel;
        }

        public void setPubChannel(String pubChannel) {
            this.pubChannel = pubChannel;
        }

        public String getStreamKey() {
            return streamKey;
        }

        public void setStreamKey(String streamKey) {
            this.streamKey = streamKey;
        }

        public int getStreamRecoveryPollMs() {
            return streamRecoveryPollMs;
        }

        public void setStreamRecoveryPollMs(int streamRecoveryPollMs) {
            this.streamRecoveryPollMs = streamRecoveryPollMs;
        }

        public String getStreamStartId() {
            return streamStartId;
        }

        public void setStreamStartId(String streamStartId) {
            this.streamStartId = streamStartId;
        }
    }

    public static class Dictionary {

        @NotBlank
        private String code;

        private boolean enabled = true;

        @NotBlank
        private String loadSql;

        private String driftCheckSql;

        @Valid
        private Apply apply = new Apply();

        private String reloadOnEvent = "FULL";

        public String getCode() {
            return code;
        }

        public void setCode(String code) {
            this.code = code;
        }

        public boolean isEnabled() {
            return enabled;
        }

        public void setEnabled(boolean enabled) {
            this.enabled = enabled;
        }

        public String getLoadSql() {
            return loadSql;
        }

        public void setLoadSql(String loadSql) {
            this.loadSql = loadSql;
        }

        public String getDriftCheckSql() {
            return driftCheckSql;
        }

        public void setDriftCheckSql(String driftCheckSql) {
            this.driftCheckSql = driftCheckSql;
        }

        public Apply getApply() {
            return apply;
        }

        public void setApply(Apply apply) {
            this.apply = apply;
        }

        public String getReloadOnEvent() {
            return reloadOnEvent;
        }

        public void setReloadOnEvent(String reloadOnEvent) {
            this.reloadOnEvent = reloadOnEvent;
        }
    }

    public static class Apply {

        private String mode = "SQL_TEMPLATE";
        private String upsertSql;
        private String deleteSql;
        private String snapshotReplaceSql;
        private String snapshotStrategy = "FULL_REPLACE";

        public String getMode() {
            return mode;
        }

        public void setMode(String mode) {
            this.mode = mode;
        }

        public String getUpsertSql() {
            return upsertSql;
        }

        public void setUpsertSql(String upsertSql) {
            this.upsertSql = upsertSql;
        }

        public String getDeleteSql() {
            return deleteSql;
        }

        public void setDeleteSql(String deleteSql) {
            this.deleteSql = deleteSql;
        }

        public String getSnapshotReplaceSql() {
            return snapshotReplaceSql;
        }

        public void setSnapshotReplaceSql(String snapshotReplaceSql) {
            this.snapshotReplaceSql = snapshotReplaceSql;
        }

        public String getSnapshotStrategy() {
            return snapshotStrategy;
        }

        public void setSnapshotStrategy(String snapshotStrategy) {
            this.snapshotStrategy = snapshotStrategy;
        }
    }
}
