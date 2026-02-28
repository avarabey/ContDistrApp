package com.contdistrapp.refdata;

import com.contdistrapp.refdata.support.RelationalTestDataSeeder;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.MvcResult;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@SpringBootTest
@AutoConfigureMockMvc
class RelationalSchemaApiTests {

    private static final String TENANT = "tenant-relational-autotest";
    private static final String DICT_CODE = "REL_TASK";
    private static final int SEED_ROWS = 1000;

    @Autowired
    private MockMvc mockMvc;

    @Autowired
    private ObjectMapper objectMapper;

    @Autowired
    private RelationalTestDataSeeder seeder;

    @BeforeEach
    void setUp() {
        seeder.recreateAndSeed(TENANT, SEED_ROWS);
    }

    @Test
    void at20_allTenTablesContain1000Rows() {
        Map<String, Integer> counts = seeder.countByTable(TENANT);
        assertThat(counts)
                .hasSize(10)
                .allSatisfy((table, count) ->
                        assertThat(count)
                                .as("Expected 1000 rows in %s", table)
                                .isEqualTo(SEED_ROWS));
    }

    @Test
    void at21_relatedTableCrudChangesApiResponseBeforeAndAfterEachStep() throws Exception {
        String key = "2001";
        JsonNode insertPayload = objectMapper.readTree("""
                {"stage":"INSERTED","priority":3,"owner":"qa"}
                """);
        JsonNode updatePayload = objectMapper.readTree("""
                {"stage":"UPDATED","priority":7,"owner":"qa","flag":true}
                """);

        JsonNode beforeInsertItem = fetchItemPayload(TENANT, DICT_CODE, key, null);
        int beforeInsertCount = fetchAllCount(TENANT, DICT_CODE, null);
        assertThat(beforeInsertItem.isNull()).isTrue();
        assertThat(beforeInsertCount).isEqualTo(SEED_ROWS);

        long insertVersion = submitDeltaAndWaitCommitted(TENANT, DICT_CODE, key, "UPSERT", insertPayload);

        JsonNode afterInsertItem = fetchItemPayload(TENANT, DICT_CODE, key, insertVersion);
        int afterInsertCount = fetchAllCount(TENANT, DICT_CODE, insertVersion);
        assertThat(afterInsertItem).isEqualTo(insertPayload);
        assertThat(afterInsertCount).isEqualTo(beforeInsertCount + 1);

        JsonNode beforeUpdateItem = fetchItemPayload(TENANT, DICT_CODE, key, insertVersion);
        assertThat(beforeUpdateItem).isEqualTo(insertPayload);

        long updateVersion = submitDeltaAndWaitCommitted(TENANT, DICT_CODE, key, "UPSERT", updatePayload);

        JsonNode afterUpdateItem = fetchItemPayload(TENANT, DICT_CODE, key, updateVersion);
        assertThat(afterUpdateItem).isEqualTo(updatePayload);
        assertThat(afterUpdateItem).isNotEqualTo(beforeUpdateItem);

        JsonNode beforeDeleteItem = fetchItemPayload(TENANT, DICT_CODE, key, updateVersion);
        int beforeDeleteCount = fetchAllCount(TENANT, DICT_CODE, updateVersion);
        assertThat(beforeDeleteItem).isEqualTo(updatePayload);
        assertThat(beforeDeleteCount).isEqualTo(SEED_ROWS + 1);

        long deleteVersion = submitDeltaAndWaitCommitted(TENANT, DICT_CODE, key, "DELETE", null);

        JsonNode afterDeleteItem = fetchItemPayload(TENANT, DICT_CODE, key, deleteVersion);
        int afterDeleteCount = fetchAllCount(TENANT, DICT_CODE, deleteVersion);
        assertThat(afterDeleteItem.isNull()).isTrue();
        assertThat(afterDeleteCount).isEqualTo(SEED_ROWS);
    }

    private JsonNode fetchItemPayload(String tenantId, String dictCode, String key, Long minVersion) throws Exception {
        var request = get("/v1/tenants/{tenantId}/dictionaries/{dictCode}/items/{key}", tenantId, dictCode, key);
        if (minVersion != null) {
            request.header("X-Min-Version", minVersion);
        }
        MvcResult response = mockMvc.perform(request)
                .andExpect(result ->
                        assertThat(result.getResponse().getStatus()).isIn(200, 404))
                .andReturn();
        if (response.getResponse().getStatus() == 404) {
            return objectMapper.nullNode();
        }
        JsonNode body = objectMapper.readTree(response.getResponse().getContentAsString());
        return body.path("payload");
    }

    private int fetchAllCount(String tenantId, String dictCode, Long minVersion) throws Exception {
        var request = get("/v1/tenants/{tenantId}/dictionaries/{dictCode}/all", tenantId, dictCode);
        if (minVersion != null) {
            request.header("X-Min-Version", minVersion);
        }
        MvcResult response = mockMvc.perform(request)
                .andExpect(status().isOk())
                .andReturn();
        JsonNode body = objectMapper.readTree(response.getResponse().getContentAsString());
        return body.path("items").size();
    }

    private long submitDeltaAndWaitCommitted(
            String tenantId,
            String dictCode,
            String key,
            String op,
            JsonNode payload
    ) throws Exception {
        ObjectNode item = objectMapper.createObjectNode();
        item.put("key", key);
        item.put("op", op);
        if (payload == null) {
            item.putNull("payload");
        } else {
            item.set("payload", payload);
        }

        ArrayNode items = objectMapper.createArrayNode().add(item);
        ObjectNode request = objectMapper.createObjectNode();
        request.put("dictCode", dictCode);
        request.put("eventType", "DELTA");
        request.set("items", items);

        MvcResult submit = mockMvc.perform(post("/v1/tenants/{tenantId}/updates", tenantId)
                        .param("consistencyMode", "WAIT_COMMIT")
                        .param("timeoutMs", "1000")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(objectMapper.writeValueAsString(request)))
                .andExpect(result ->
                        assertThat(result.getResponse().getStatus()).isIn(200, 202))
                .andReturn();

        String eventId = objectMapper.readTree(submit.getResponse().getContentAsString())
                .path("eventId")
                .asText();
        return waitCommitted(tenantId, eventId);
    }

    private long waitCommitted(String tenantId, String eventId) throws Exception {
        for (int i = 0; i < 150; i++) {
            MvcResult statusResponse = mockMvc.perform(get("/v1/tenants/{tenantId}/updates/{eventId}", tenantId, eventId))
                    .andExpect(status().isOk())
                    .andReturn();
            JsonNode statusBody = objectMapper.readTree(statusResponse.getResponse().getContentAsString());
            String status = statusBody.path("status").asText();
            if ("COMMITTED".equals(status)) {
                return statusBody.path("committedVersion").asLong();
            }
            if ("FAILED".equals(status)) {
                throw new IllegalStateException("Update failed: " + statusBody);
            }
            Thread.sleep(20);
        }
        throw new IllegalStateException("Update did not commit in time for eventId=" + eventId);
    }
}
