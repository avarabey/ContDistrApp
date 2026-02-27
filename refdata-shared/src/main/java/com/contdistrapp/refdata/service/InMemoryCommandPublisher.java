package com.contdistrapp.refdata.service;

import com.contdistrapp.refdata.domain.UpdateCommand;
import com.contdistrapp.refdata.config.role.ConditionalOnRefdataRole;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.stereotype.Component;

@Component
@ConditionalOnProperty(prefix = "refdata.kafka", name = "enabled", havingValue = "false", matchIfMissing = true)
@ConditionalOnRefdataRole({"command-api"})
public class InMemoryCommandPublisher implements CommandPublisher {

    private final ObjectProvider<CommandDispatcher> dispatcherProvider;

    public InMemoryCommandPublisher(ObjectProvider<CommandDispatcher> dispatcherProvider) {
        this.dispatcherProvider = dispatcherProvider;
    }

    @Override
    public void publish(UpdateCommand command) {
        CommandDispatcher dispatcher = dispatcherProvider.getIfAvailable();
        if (dispatcher == null) {
            throw new IllegalStateException("In-memory publisher requires apply pipeline in the same process");
        }
        dispatcher.dispatch(command);
    }
}
