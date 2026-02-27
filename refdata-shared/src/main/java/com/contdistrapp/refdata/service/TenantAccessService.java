package com.contdistrapp.refdata.service;

import com.contdistrapp.refdata.config.role.ConditionalOnRefdataRole;
import com.contdistrapp.refdata.error.ForbiddenException;
import org.springframework.stereotype.Service;

@Service
@ConditionalOnRefdataRole({"command-api", "query-api"})
public class TenantAccessService {

    public void assertAllowed(String tenantIdFromPath, String tenantIdFromAuthHeader) {
        if (tenantIdFromAuthHeader == null || tenantIdFromAuthHeader.isBlank()) {
            return;
        }
        if (!tenantIdFromPath.equals(tenantIdFromAuthHeader)) {
            throw new ForbiddenException("tenantId from path does not match authenticated tenant");
        }
    }
}
