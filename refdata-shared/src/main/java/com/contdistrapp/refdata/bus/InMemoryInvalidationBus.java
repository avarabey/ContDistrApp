package com.contdistrapp.refdata.bus;

import com.contdistrapp.refdata.config.role.ConditionalOnRefdataRole;
import com.contdistrapp.refdata.domain.InvalidationEvent;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Consumer;

@Component
@ConditionalOnProperty(prefix = "refdata.redis", name = "enabled", havingValue = "false", matchIfMissing = true)
@ConditionalOnRefdataRole({"query-api", "outbox-relay"})
public class InMemoryInvalidationBus implements InvalidationBus {

    private final List<Consumer<InvalidationEvent>> listeners = new CopyOnWriteArrayList<>();

    @Override
    public void publish(InvalidationEvent event) {
        for (Consumer<InvalidationEvent> listener : listeners) {
            listener.accept(event);
        }
    }

    @Override
    public void subscribe(Consumer<InvalidationEvent> listener) {
        listeners.add(listener);
    }
}
