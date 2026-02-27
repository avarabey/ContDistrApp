package com.contdistrapp.refdata.service;

import com.contdistrapp.refdata.config.RefDataProperties;
import org.springframework.stereotype.Component;

@Component
public class RefDataTimeouts {

    private final RefDataProperties properties;

    public RefDataTimeouts(RefDataProperties properties) {
        this.properties = properties;
    }

    public int waitForReloadMs() {
        return properties.getQuery().getWaitForReloadMs();
    }

    public int outboxPollIntervalMs() {
        return properties.getOutbox().getPollIntervalMs();
    }

    public int outboxBatchSize() {
        return properties.getOutbox().getBatchSize();
    }

    public int redisStreamRecoveryPollMs() {
        return properties.getRedis().getStreamRecoveryPollMs();
    }
}
