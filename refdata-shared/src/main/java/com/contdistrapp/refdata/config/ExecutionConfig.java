package com.contdistrapp.refdata.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

@Configuration
public class ExecutionConfig {

    @Bean(destroyMethod = "shutdown")
    public ExecutorService cacheReloadExecutor(RefDataProperties properties) {
        return Executors.newFixedThreadPool(properties.getCache().getReloadParallelism());
    }
}
