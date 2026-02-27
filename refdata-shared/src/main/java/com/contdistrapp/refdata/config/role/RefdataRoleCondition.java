package com.contdistrapp.refdata.config.role;

import org.springframework.context.annotation.Condition;
import org.springframework.context.annotation.ConditionContext;
import org.springframework.core.annotation.MergedAnnotation;
import org.springframework.core.type.AnnotatedTypeMetadata;

import java.util.Arrays;
import java.util.Locale;
import java.util.Set;
import java.util.stream.Collectors;

public class RefdataRoleCondition implements Condition {

    @Override
    public boolean matches(ConditionContext context, AnnotatedTypeMetadata metadata) {
        String activeRole = context.getEnvironment().getProperty("refdata.role", "all")
                .toLowerCase(Locale.ROOT);

        if ("all".equals(activeRole)) {
            return true;
        }

        MergedAnnotation<ConditionalOnRefdataRole> annotation = metadata.getAnnotations()
                .get(ConditionalOnRefdataRole.class);
        if (!annotation.isPresent()) {
            return true;
        }

        String[] allowedRaw = annotation.getStringArray("value");
        Set<String> allowed = Arrays.stream(allowedRaw)
                .map(v -> v.toLowerCase(Locale.ROOT))
                .collect(Collectors.toSet());

        return allowed.contains(activeRole);
    }
}
