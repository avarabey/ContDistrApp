package com.contdistrapp.refdata;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.MvcResult;

import static org.assertj.core.api.Assertions.assertThat;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.header;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@SpringBootTest
@AutoConfigureMockMvc
class RefdataPlatformApplicationTests {

    @Autowired
    private MockMvc mockMvc;

    @Autowired
    private ObjectMapper objectMapper;

    @Test
    void at01_asyncWriteAcceptedAndCommitted() throws Exception {
        String eventId = submitUpdate("tenant-a", "ASYNC", """
                {
                  "dictCode": "COUNTRY",
                  "eventType": "DELTA",
                  "items": [{"key":"RU","op":"UPSERT","payload":{"name":"Russia"}}]
                }
                """);

        waitCommitted("tenant-a", eventId);
    }

    @Test
    void at02_waitCommitSuccessAndReadWithBarrier() throws Exception {
        MvcResult response = mockMvc.perform(post("/v1/tenants/tenant-a/updates")
                        .param("consistencyMode", "WAIT_COMMIT")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("""
                                {
                                  "dictCode": "COUNTRY",
                                  "eventType": "DELTA",
                                  "items": [{"key":"DE","op":"UPSERT","payload":{"name":"Germany"}}]
                                }
                                """))
                .andExpect(status().isOk())
                .andReturn();

        JsonNode json = objectMapper.readTree(response.getResponse().getContentAsString());
        long committedVersion = json.path("committedVersion").asLong();
        assertThat(committedVersion).isPositive();

        mockMvc.perform(get("/v1/tenants/tenant-a/dictionaries/COUNTRY/items/DE")
                        .header("X-Min-Version", committedVersion))
                .andExpect(status().isOk())
                .andExpect(header().exists("X-Dict-Version"))
                .andExpect(header().exists("X-Data-Source"));
    }

    @Test
    void at04_versionBarrierNotCommitted() throws Exception {
        mockMvc.perform(get("/v1/tenants/tenant-a/dictionaries/COUNTRY/all")
                        .header("X-Min-Version", 9999L))
                .andExpect(status().isConflict());
    }

    @Test
    void at05_snapshotFullReplaceSemantics() throws Exception {
        String firstEvent = submitUpdate("tenant-b", "WAIT_COMMIT", """
                {
                  "dictCode": "COUNTRY",
                  "eventType": "DELTA",
                  "items": [
                    {"key":"RU","op":"UPSERT","payload":{"name":"Russia"}},
                    {"key":"US","op":"UPSERT","payload":{"name":"USA"}}
                  ]
                }
                """);
        waitCommitted("tenant-b", firstEvent);

        String snapshotEvent = submitUpdate("tenant-b", "WAIT_COMMIT", """
                {
                  "dictCode": "COUNTRY",
                  "eventType": "SNAPSHOT",
                  "items": [
                    {"key":"RU","op":"UPSERT","payload":{"name":"Russia"}}
                  ]
                }
                """);
        waitCommitted("tenant-b", snapshotEvent);

        for (int i = 0; i < 100; i++) {
            MvcResult all = mockMvc.perform(get("/v1/tenants/tenant-b/dictionaries/COUNTRY/all"))
                    .andExpect(status().isOk())
                    .andReturn();

            JsonNode items = objectMapper.readTree(all.getResponse().getContentAsString()).path("items");
            if (items.has("RU") && !items.has("US")) {
                return;
            }
            Thread.sleep(20);
        }

        throw new IllegalStateException("Snapshot full-replace not observed in cache");
    }

    @Test
    void at10_tenantIsolation() throws Exception {
        mockMvc.perform(get("/v1/tenants/tenant-a/dictionaries/COUNTRY/version")
                        .header("X-Auth-Tenant", "tenant-b"))
                .andExpect(status().isForbidden());
    }

    private String submitUpdate(String tenantId, String consistencyMode, String payload) throws Exception {
        MvcResult response = mockMvc.perform(post("/v1/tenants/{tenantId}/updates", tenantId)
                        .param("consistencyMode", consistencyMode)
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(payload))
                .andExpect(result ->
                        assertThat(result.getResponse().getStatus()).isIn(200, 202))
                .andReturn();

        JsonNode json = objectMapper.readTree(response.getResponse().getContentAsString());
        return json.path("eventId").asText();
    }

    private void waitCommitted(String tenantId, String eventId) throws Exception {
        for (int i = 0; i < 100; i++) {
            MvcResult response = mockMvc.perform(get("/v1/tenants/{tenantId}/updates/{eventId}", tenantId, eventId))
                    .andExpect(status().isOk())
                    .andReturn();
            JsonNode json = objectMapper.readTree(response.getResponse().getContentAsString());
            String status = json.path("status").asText();
            if ("COMMITTED".equals(status)) {
                return;
            }
            if ("FAILED".equals(status)) {
                throw new IllegalStateException("Update failed: " + json);
            }
            Thread.sleep(20);
        }
        throw new IllegalStateException("Update did not commit in time");
    }
}
