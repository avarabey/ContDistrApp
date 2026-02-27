package com.contdistrapp.refdata.api;

import com.contdistrapp.refdata.config.role.ConditionalOnRefdataRole;
import com.contdistrapp.refdata.domain.ConsistencyMode;
import com.contdistrapp.refdata.domain.UpdateStatus;
import com.contdistrapp.refdata.error.BadRequestException;
import com.contdistrapp.refdata.service.TenantAccessService;
import com.contdistrapp.refdata.service.UpdateCommandService;
import jakarta.validation.Valid;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/v1/tenants/{tenantId}")
@ConditionalOnRefdataRole({"command-api"})
public class UpdateController {

    private final TenantAccessService tenantAccessService;
    private final UpdateCommandService updateCommandService;

    public UpdateController(TenantAccessService tenantAccessService, UpdateCommandService updateCommandService) {
        this.tenantAccessService = tenantAccessService;
        this.updateCommandService = updateCommandService;
    }

    @PostMapping("/updates")
    public ResponseEntity<UpdateSubmissionResponse> submit(
            @PathVariable String tenantId,
            @RequestParam(defaultValue = "ASYNC") ConsistencyMode consistencyMode,
            @RequestParam(required = false) Integer timeoutMs,
            @RequestHeader(value = "X-Auth-Tenant", required = false) String authTenant,
            @Valid @RequestBody UpdateCommandRequest request
    ) {
        tenantAccessService.assertAllowed(tenantId, authTenant);
        if (timeoutMs != null && (timeoutMs < 50 || timeoutMs > 1000)) {
            throw new BadRequestException("timeoutMs must be in range 50..1000");
        }

        UpdateSubmissionResponse response = updateCommandService.submit(tenantId, consistencyMode, timeoutMs, request);
        if (consistencyMode == ConsistencyMode.WAIT_COMMIT && response.status() == UpdateStatus.COMMITTED) {
            return ResponseEntity.ok(response);
        }
        return ResponseEntity.status(HttpStatus.ACCEPTED).body(response);
    }

    @GetMapping("/updates/{eventId}")
    public UpdateStatusResponse status(
            @PathVariable String tenantId,
            @PathVariable String eventId,
            @RequestHeader(value = "X-Auth-Tenant", required = false) String authTenant
    ) {
        tenantAccessService.assertAllowed(tenantId, authTenant);
        return updateCommandService.status(tenantId, eventId);
    }
}
