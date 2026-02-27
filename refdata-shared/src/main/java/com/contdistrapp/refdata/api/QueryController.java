package com.contdistrapp.refdata.api;

import com.contdistrapp.refdata.config.role.ConditionalOnRefdataRole;
import com.contdistrapp.refdata.domain.DataSourceType;
import com.contdistrapp.refdata.service.QueryReadResult;
import com.contdistrapp.refdata.service.QueryService;
import com.contdistrapp.refdata.service.TenantAccessService;
import org.springframework.http.HttpHeaders;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.Arrays;
import java.util.List;

@RestController
@RequestMapping("/v1/tenants/{tenantId}/dictionaries/{dictCode}")
@ConditionalOnRefdataRole({"query-api"})
public class QueryController {

    private static final String HEADER_MIN_VERSION = "X-Min-Version";
    private static final String HEADER_DICT_VERSION = "X-Dict-Version";
    private static final String HEADER_DATA_SOURCE = "X-Data-Source";

    private final TenantAccessService tenantAccessService;
    private final QueryService queryService;

    public QueryController(TenantAccessService tenantAccessService, QueryService queryService) {
        this.tenantAccessService = tenantAccessService;
        this.queryService = queryService;
    }

    @GetMapping("/items/{key}")
    public ResponseEntity<DictionaryItemResponse> item(
            @PathVariable String tenantId,
            @PathVariable String dictCode,
            @PathVariable String key,
            @RequestHeader(value = HEADER_MIN_VERSION, required = false) Long minVersion,
            @RequestHeader(value = "X-Auth-Tenant", required = false) String authTenant
    ) {
        tenantAccessService.assertAllowed(tenantId, authTenant);
        QueryReadResult result = queryService.readItem(tenantId, dictCode, key, minVersion);
        DictionaryItemResponse body = new DictionaryItemResponse(key, result.items().get(key));
        return withHeaders(result).body(body);
    }

    @GetMapping("/items")
    public ResponseEntity<DictionaryItemsResponse> items(
            @PathVariable String tenantId,
            @PathVariable String dictCode,
            @RequestParam String keys,
            @RequestHeader(value = HEADER_MIN_VERSION, required = false) Long minVersion,
            @RequestHeader(value = "X-Auth-Tenant", required = false) String authTenant
    ) {
        tenantAccessService.assertAllowed(tenantId, authTenant);
        List<String> parsedKeys = Arrays.stream(keys.split(","))
                .map(String::trim)
                .filter(v -> !v.isBlank())
                .toList();
        QueryReadResult result = queryService.readItems(tenantId, dictCode, parsedKeys, minVersion);
        return withHeaders(result).body(new DictionaryItemsResponse(result.items()));
    }

    @GetMapping("/all")
    public ResponseEntity<DictionaryItemsResponse> all(
            @PathVariable String tenantId,
            @PathVariable String dictCode,
            @RequestHeader(value = HEADER_MIN_VERSION, required = false) Long minVersion,
            @RequestHeader(value = "X-Auth-Tenant", required = false) String authTenant
    ) {
        tenantAccessService.assertAllowed(tenantId, authTenant);
        QueryReadResult result = queryService.readAll(tenantId, dictCode, minVersion);
        return withHeaders(result).body(new DictionaryItemsResponse(result.items()));
    }

    @GetMapping("/version")
    public ResponseEntity<DictionaryVersionResponse> version(
            @PathVariable String tenantId,
            @PathVariable String dictCode,
            @RequestHeader(value = "X-Auth-Tenant", required = false) String authTenant
    ) {
        tenantAccessService.assertAllowed(tenantId, authTenant);
        long version = queryService.currentVersion(tenantId, dictCode);
        QueryReadResult result = new QueryReadResult(version, DataSourceType.MEMORY, java.util.Map.of());
        return withHeaders(result).body(new DictionaryVersionResponse(version));
    }

    private ResponseEntity.BodyBuilder withHeaders(QueryReadResult result) {
        HttpHeaders headers = new HttpHeaders();
        headers.add(HEADER_DICT_VERSION, Long.toString(result.version()));
        headers.add(HEADER_DATA_SOURCE, result.sourceType() == DataSourceType.MEMORY ? "memory" : "postgres_fallback");
        return ResponseEntity.ok().headers(headers);
    }
}
