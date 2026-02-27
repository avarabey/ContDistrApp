package com.contdistrapp.refdata.bus;

import com.contdistrapp.refdata.config.RefDataProperties;
import com.contdistrapp.refdata.config.role.ConditionalOnRefdataRole;
import com.contdistrapp.refdata.domain.InvalidationEvent;
import com.contdistrapp.refdata.service.RefDataTimeouts;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.annotation.PostConstruct;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.data.domain.Range;
import org.springframework.data.redis.connection.Message;
import org.springframework.data.redis.connection.MessageListener;
import org.springframework.data.redis.connection.stream.MapRecord;
import org.springframework.data.redis.connection.stream.RecordId;
import org.springframework.data.redis.connection.stream.StreamRecords;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.listener.ChannelTopic;
import org.springframework.data.redis.listener.RedisMessageListenerContainer;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Consumer;

@Component
@ConditionalOnProperty(prefix = "refdata.redis", name = "enabled", havingValue = "true")
@ConditionalOnRefdataRole({"query-api", "outbox-relay"})
public class RedisInvalidationBus implements InvalidationBus, MessageListener {

    private static final Logger log = LoggerFactory.getLogger(RedisInvalidationBus.class);

    private final StringRedisTemplate redisTemplate;
    private final RedisMessageListenerContainer listenerContainer;
    private final RefDataProperties properties;
    private final RefDataTimeouts timeouts;
    private final ObjectMapper objectMapper;
    private final List<Consumer<InvalidationEvent>> listeners = new CopyOnWriteArrayList<>();

    private volatile String lastStreamId;

    public RedisInvalidationBus(
            StringRedisTemplate redisTemplate,
            RedisMessageListenerContainer listenerContainer,
            RefDataProperties properties,
            RefDataTimeouts timeouts,
            ObjectMapper objectMapper
    ) {
        this.redisTemplate = redisTemplate;
        this.listenerContainer = listenerContainer;
        this.properties = properties;
        this.timeouts = timeouts;
        this.objectMapper = objectMapper;
        this.lastStreamId = properties.getRedis().getStreamStartId();
    }

    @PostConstruct
    public void init() {
        listenerContainer.addMessageListener(this, new ChannelTopic(properties.getRedis().getPubChannel()));
    }

    @Override
    public void publish(InvalidationEvent event) {
        String payload = toJson(event);
        redisTemplate.convertAndSend(properties.getRedis().getPubChannel(), payload);

        var record = StreamRecords
                .mapBacked(Map.of("event", payload))
                .withStreamKey(properties.getRedis().getStreamKey());
        RecordId id = redisTemplate.opsForStream().add(record);
        if (id != null) {
            lastStreamId = id.getValue();
        }
    }

    @Override
    public void subscribe(Consumer<InvalidationEvent> listener) {
        listeners.add(listener);
    }

    @Override
    public void onMessage(Message message, byte[] pattern) {
        String payload = new String(message.getBody(), StandardCharsets.UTF_8);
        dispatchPayload(payload);
    }

    @Scheduled(fixedDelayString = "#{@refDataTimeouts.redisStreamRecoveryPollMs()}")
    public void recoverFromStream() {
        try {
            List<MapRecord<String, Object, Object>> records = redisTemplate.opsForStream().range(
                    properties.getRedis().getStreamKey(),
                    Range.rightOpen(lastStreamId, "+")
            );

            for (MapRecord<String, Object, Object> record : records) {
                Object payload = record.getValue().get("event");
                if (payload != null) {
                    dispatchPayload(payload.toString());
                }
                lastStreamId = record.getId().getValue();
            }
        } catch (Exception ex) {
            log.error("Redis stream recovery failed", ex);
        }
    }

    private void dispatchPayload(String payload) {
        try {
            InvalidationEvent event = objectMapper.readValue(payload, InvalidationEvent.class);
            for (Consumer<InvalidationEvent> listener : listeners) {
                listener.accept(event);
            }
        } catch (Exception ex) {
            log.error("Failed to decode invalidation payload from Redis", ex);
        }
    }

    private String toJson(InvalidationEvent event) {
        try {
            return objectMapper.writeValueAsString(event);
        } catch (JsonProcessingException e) {
            throw new IllegalStateException("Unable to serialize invalidation event", e);
        }
    }
}
