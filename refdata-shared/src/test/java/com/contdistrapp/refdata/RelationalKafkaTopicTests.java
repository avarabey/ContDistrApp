package com.contdistrapp.refdata;

import com.contdistrapp.refdata.support.RelationalTestDataSeeder;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.http.MediaType;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.MvcResult;

import java.time.Duration;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@SpringBootTest(properties = {
        "refdata.kafka.enabled=true",
        "spring.kafka.bootstrap-servers=${spring.embedded.kafka.brokers}"
})
@AutoConfigureMockMvc
@EmbeddedKafka(partitions = 1, topics = {"refdata.commands"})
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_CLASS)
class RelationalKafkaTopicTests {

    private static final String TENANT = "tenant-relational-kafka-autotest";
    private static final String DICT_CODE = "REL_TASK";
    private static final int SEED_ROWS = 1000;

    @Autowired
    private MockMvc mockMvc;

    @Autowired
    private ObjectMapper objectMapper;

    @Autowired
    private RelationalTestDataSeeder seeder;

    @Autowired
    private EmbeddedKafkaBroker embeddedKafkaBroker;

    @BeforeEach
    void setUp() {
        seeder.recreateAndSeed(TENANT, SEED_ROWS);
    }

    @Test
    void at22_commandIsPublishedToKafkaTopicAndAppliedToApiReadModel() throws Exception {
        String key = "3001";
        JsonNode payload = objectMapper.readTree("""
                {"stage":"KAFKA","priority":9}
                """);

        Map<String, Object> consumerProps = Map.of(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, embeddedKafkaBroker.getBrokersAsString(),
                ConsumerConfig.GROUP_ID_CONFIG, "kafka-autotest-audit-group",
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest",
                ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true"
        );

        Consumer<String, String> consumer = new DefaultKafkaConsumerFactory<>(
                consumerProps,
                new StringDeserializer(),
                new StringDeserializer()
        ).createConsumer();
        embeddedKafkaBroker.consumeFromAnEmbeddedTopic(consumer, "refdata.commands");

        try {
            JsonNode beforeInsert = fetchItemPayload(TENANT, DICT_CODE, key, null);
            assertThat(beforeInsert.isNull()).isTrue();

            SubmitResult submitResult = submitDeltaAndWaitCommitted(TENANT, DICT_CODE, key, "UPSERT", payload);

            JsonNode afterInsert = fetchItemPayload(TENANT, DICT_CODE, key, submitResult.committedVersion());
            assertThat(afterInsert).isEqualTo(payload);

            ConsumerRecord<String, String> topicRecord = awaitRecord(consumer, "refdata.commands", submitResult.eventId());
            assertThat(topicRecord).isNotNull();
            assertThat(topicRecord.value()).contains("\"dictCode\":\"REL_TASK\"");
            assertThat(topicRecord.value()).contains("\"eventId\":\"" + submitResult.eventId() + "\"");
        } finally {
            consumer.close(Duration.ofSeconds(2));
        }
    }

    private ConsumerRecord<String, String> awaitRecord(Consumer<String, String> consumer, String topic, String eventId) {
        for (int i = 0; i < 20; i++) {
            var polled = consumer.poll(Duration.ofMillis(500));
            for (ConsumerRecord<String, String> record : polled.records(topic)) {
                if (record.value() != null && record.value().contains(eventId)) {
                    return record;
                }
            }
        }
        return null;
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

    private SubmitResult submitDeltaAndWaitCommitted(
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
        long committedVersion = waitCommitted(tenantId, eventId);
        return new SubmitResult(eventId, committedVersion);
    }

    private long waitCommitted(String tenantId, String eventId) throws Exception {
        for (int i = 0; i < 200; i++) {
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

    private record SubmitResult(String eventId, long committedVersion) {
    }
}
