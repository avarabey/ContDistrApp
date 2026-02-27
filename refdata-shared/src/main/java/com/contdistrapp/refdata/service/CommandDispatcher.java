package com.contdistrapp.refdata.service;

import com.contdistrapp.refdata.config.role.ConditionalOnRefdataRole;
import com.contdistrapp.refdata.domain.UpdateCommand;
import jakarta.annotation.PreDestroy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

@Component
@ConditionalOnRefdataRole({"apply-service"})
public class CommandDispatcher {

    private static final Logger log = LoggerFactory.getLogger(CommandDispatcher.class);

    private final ApplyProcessor applyProcessor;
    private final Map<String, ExecutorService> partitionExecutors = new ConcurrentHashMap<>();

    public CommandDispatcher(ApplyProcessor applyProcessor) {
        this.applyProcessor = applyProcessor;
    }

    public void dispatch(UpdateCommand command) {
        ExecutorService executor = partitionExecutors.computeIfAbsent(
                command.partitionKey(),
                key -> Executors.newSingleThreadExecutor(r -> {
                    Thread t = new Thread(r);
                    t.setName("apply-" + key.replace(':', '_'));
                    t.setDaemon(true);
                    return t;
                })
        );

        executor.submit(() -> {
            try {
                applyProcessor.process(command);
            } catch (Exception ex) {
                log.error("Failed to process eventId={} tenant={} dict={}", command.eventId(), command.tenantId(), command.dictCode(), ex);
                applyProcessor.fail(command, ex.getMessage());
            }
        });
    }

    @PreDestroy
    public void shutdown() {
        for (ExecutorService executor : partitionExecutors.values()) {
            executor.shutdown();
        }
    }
}
