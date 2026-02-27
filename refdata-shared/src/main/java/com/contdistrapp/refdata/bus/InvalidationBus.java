package com.contdistrapp.refdata.bus;

import com.contdistrapp.refdata.domain.InvalidationEvent;

import java.util.function.Consumer;

public interface InvalidationBus {

    void publish(InvalidationEvent event);

    void subscribe(Consumer<InvalidationEvent> listener);
}
