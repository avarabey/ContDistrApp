package com.contdistrapp.refdata.support;

import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.stereotype.Component;

import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalTime;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.UUID;

@Component
public class RelationalTestDataSeeder {

    private final NamedParameterJdbcTemplate jdbc;

    public RelationalTestDataSeeder(NamedParameterJdbcTemplate jdbc) {
        this.jdbc = jdbc;
    }

    public void recreateAndSeed(String tenantId, int size) {
        clearRelationalDomain();
        seedOrgUnits(tenantId, size);
        seedEmployees(tenantId, size);
        seedEmployeeProfiles(tenantId, size);
        seedProjects(tenantId, size);
        seedTasks(tenantId, size);
        seedTags(tenantId, size);
        seedTaskTags(tenantId, size);
        seedAssets(tenantId, size);
        seedInvoices(tenantId, size);
        seedProjectDependencies(tenantId, size);
        ensureDictionaryVersion(tenantId, "REL_TASK", 1L);
    }

    public Map<String, Integer> countByTable(String tenantId) {
        Map<String, Integer> counts = new LinkedHashMap<>();
        counts.put("rel_org_unit", count(tenantId, "rel_org_unit"));
        counts.put("rel_employee", count(tenantId, "rel_employee"));
        counts.put("rel_employee_profile", count(tenantId, "rel_employee_profile"));
        counts.put("rel_project", count(tenantId, "rel_project"));
        counts.put("rel_task", count(tenantId, "rel_task"));
        counts.put("rel_tag", count(tenantId, "rel_tag"));
        counts.put("rel_task_tag", count(tenantId, "rel_task_tag"));
        counts.put("rel_asset", count(tenantId, "rel_asset"));
        counts.put("rel_invoice", count(tenantId, "rel_invoice"));
        counts.put("rel_project_dependency", count(tenantId, "rel_project_dependency"));
        return counts;
    }

    private int count(String tenantId, String table) {
        MapSqlParameterSource params = new MapSqlParameterSource().addValue("tenantId", tenantId);
        Integer value = jdbc.queryForObject(
                "select count(*) from " + table + " where tenant_id = :tenantId",
                params,
                Integer.class
        );
        return value == null ? 0 : value;
    }

    private void clearRelationalDomain() {
        jdbc.getJdbcTemplate().execute("delete from rel_project_dependency");
        jdbc.getJdbcTemplate().execute("delete from rel_invoice");
        jdbc.getJdbcTemplate().execute("delete from rel_asset");
        jdbc.getJdbcTemplate().execute("delete from rel_task_tag");
        jdbc.getJdbcTemplate().execute("delete from rel_tag");
        jdbc.getJdbcTemplate().execute("delete from rel_task");
        jdbc.getJdbcTemplate().execute("delete from rel_project");
        jdbc.getJdbcTemplate().execute("delete from rel_employee_profile");
        jdbc.getJdbcTemplate().execute("delete from rel_employee");
        jdbc.getJdbcTemplate().execute("delete from rel_org_unit");
    }

    private void seedOrgUnits(String tenantId, int size) {
        Instant base = Instant.parse("2025-01-01T00:00:00Z");
        for (int i = 1; i <= size; i++) {
            MapSqlParameterSource params = new MapSqlParameterSource()
                    .addValue("id", (long) i)
                    .addValue("tenantId", tenantId)
                    .addValue("parentId", i == 1 ? null : (long) (i / 2))
                    .addValue("unitCode", "U%07d".formatted(i))
                    .addValue("unitName", "Org Unit " + i)
                    .addValue("active", i % 11 != 0)
                    .addValue("createdAt", base.plusSeconds(i * 60L));
            jdbc.update("""
                    insert into rel_org_unit(id, tenant_id, parent_id, unit_code, unit_name, active, created_at)
                    values (:id, :tenantId, :parentId, :unitCode, :unitName, :active, :createdAt)
                    """, params);
        }
    }

    private void seedEmployees(String tenantId, int size) {
        Instant base = Instant.parse("2025-01-02T00:00:00Z");
        LocalDate birthBase = LocalDate.of(1980, 1, 1);
        for (int i = 1; i <= size; i++) {
            MapSqlParameterSource params = new MapSqlParameterSource()
                    .addValue("id", (long) i)
                    .addValue("tenantId", tenantId)
                    .addValue("orgUnitId", (long) i)
                    .addValue("managerId", i == 1 ? null : (long) (i / 2))
                    .addValue("firstName", "First" + i)
                    .addValue("lastName", "Last" + i)
                    .addValue("email", "employee%04d@tenant.local".formatted(i))
                    .addValue("birthDate", birthBase.plusDays(i))
                    .addValue("shiftStart", LocalTime.of(i % 24, (i * 7) % 60))
                    .addValue("salary", BigDecimal.valueOf(3000L + i).setScale(2))
                    .addValue("bonusRate", (i % 20) / 100.0d)
                    .addValue("fullTime", i % 2 == 0)
                    .addValue("note", "Seeded employee " + i)
                    .addValue("createdAt", base.plusSeconds(i * 120L));
            jdbc.update("""
                    insert into rel_employee(
                        id, tenant_id, org_unit_id, manager_id, first_name, last_name, email,
                        birth_date, shift_start, salary, bonus_rate, full_time, note, created_at
                    )
                    values (
                        :id, :tenantId, :orgUnitId, :managerId, :firstName, :lastName, :email,
                        :birthDate, :shiftStart, :salary, :bonusRate, :fullTime, :note, :createdAt
                    )
                    """, params);
        }
    }

    private void seedEmployeeProfiles(String tenantId, int size) {
        Instant base = Instant.parse("2025-01-03T00:00:00Z");
        for (int i = 1; i <= size; i++) {
            UUID profileUuid = UUID.nameUUIDFromBytes(("profile-" + i).getBytes(StandardCharsets.UTF_8));
            MapSqlParameterSource params = new MapSqlParameterSource()
                    .addValue("employeeId", (long) i)
                    .addValue("tenantId", tenantId)
                    .addValue("profileUuid", profileUuid)
                    .addValue("preferredShift", (short) ((i % 3) + 1))
                    .addValue("emergencyPhone", "+100000%04d".formatted(i))
                    .addValue("bio", "Profile for employee " + i)
                    .addValue("reviewedAt", base.plusSeconds(i * 180L));
            jdbc.update("""
                    insert into rel_employee_profile(
                        employee_id, tenant_id, profile_uuid, preferred_shift, emergency_phone, bio, reviewed_at
                    )
                    values (
                        :employeeId, :tenantId, :profileUuid, :preferredShift, :emergencyPhone, :bio, :reviewedAt
                    )
                    """, params);
        }
    }

    private void seedProjects(String tenantId, int size) {
        Instant base = Instant.parse("2025-01-04T00:00:00Z");
        LocalDate startBase = LocalDate.of(2025, 1, 1);
        for (int i = 1; i <= size; i++) {
            char status = switch (i % 3) {
                case 0 -> 'C';
                case 1 -> 'N';
                default -> 'A';
            };
            LocalDate startDate = startBase.plusDays(i % 365);
            MapSqlParameterSource params = new MapSqlParameterSource()
                    .addValue("id", (long) i)
                    .addValue("tenantId", tenantId)
                    .addValue("orgUnitId", (long) i)
                    .addValue("leadEmployeeId", (long) i)
                    .addValue("projectCode", "P%06d".formatted(i))
                    .addValue("title", "Project " + i)
                    .addValue("budget", BigDecimal.valueOf(100000L + (long) i * 100L).setScale(2))
                    .addValue("startDate", startDate)
                    .addValue("endDate", startDate.plusDays(90))
                    .addValue("status", Character.toString(status))
                    .addValue("priority", (short) ((i % 5) + 1))
                    .addValue("active", i % 9 != 0)
                    .addValue("createdAt", base.plusSeconds(i * 240L));
            jdbc.update("""
                    insert into rel_project(
                        id, tenant_id, org_unit_id, lead_employee_id, project_code, title,
                        budget, start_date, end_date, status, priority, active, created_at
                    )
                    values (
                        :id, :tenantId, :orgUnitId, :leadEmployeeId, :projectCode, :title,
                        :budget, :startDate, :endDate, :status, :priority, :active, :createdAt
                    )
                    """, params);
        }
    }

    private void seedTasks(String tenantId, int size) {
        Instant base = Instant.parse("2025-01-05T00:00:00Z");
        for (int i = 1; i <= size; i++) {
            boolean completed = i % 4 == 0;
            String state = completed ? "DONE" : "OPEN";
            MapSqlParameterSource params = new MapSqlParameterSource()
                    .addValue("id", (long) i)
                    .addValue("tenantId", tenantId)
                    .addValue("projectId", (long) i)
                    .addValue("assigneeEmployeeId", (long) i)
                    .addValue("taskKey", "TASK-%06d".formatted(i))
                    .addValue("title", "Task " + i)
                    .addValue("details", "Task details " + i)
                    .addValue("estimateHours", (i % 40) + 1)
                    .addValue("progressPercent", (short) (i % 101))
                    .addValue("dueAt", base.plusSeconds(i * 300L))
                    .addValue("completed", completed)
                    .addValue("payloadJson", "{\"seed\":" + i + ",\"state\":\"" + state + "\"}")
                    .addValue("deleted", false)
                    .addValue("updatedAt", base.plusSeconds(i * 300L));
            jdbc.update("""
                    insert into rel_task(
                        id, tenant_id, project_id, assignee_employee_id, task_key, title, details,
                        estimate_hours, progress_percent, due_at, completed, payload_json, deleted, updated_at
                    )
                    values (
                        :id, :tenantId, :projectId, :assigneeEmployeeId, :taskKey, :title, :details,
                        :estimateHours, :progressPercent, :dueAt, :completed, :payloadJson, :deleted, :updatedAt
                    )
                    """, params);
        }
    }

    private void seedTags(String tenantId, int size) {
        LocalDate baseDate = LocalDate.of(2025, 1, 1);
        for (int i = 1; i <= size; i++) {
            String color = "#%06X".formatted((i * 31) % 0xFFFFFF);
            MapSqlParameterSource params = new MapSqlParameterSource()
                    .addValue("id", (long) i)
                    .addValue("tenantId", tenantId)
                    .addValue("tagName", "TAG-%06d".formatted(i))
                    .addValue("colorHex", color)
                    .addValue("systemTag", i % 10 == 0)
                    .addValue("createdOn", baseDate.plusDays(i % 365));
            jdbc.update("""
                    insert into rel_tag(id, tenant_id, tag_name, color_hex, system_tag, created_on)
                    values (:id, :tenantId, :tagName, :colorHex, :systemTag, :createdOn)
                    """, params);
        }
    }

    private void seedTaskTags(String tenantId, int size) {
        Instant base = Instant.parse("2025-01-06T00:00:00Z");
        for (int i = 1; i <= size; i++) {
            MapSqlParameterSource params = new MapSqlParameterSource()
                    .addValue("taskId", (long) i)
                    .addValue("tagId", (long) i)
                    .addValue("tenantId", tenantId)
                    .addValue("assignedAt", base.plusSeconds(i * 360L));
            jdbc.update("""
                    insert into rel_task_tag(task_id, tag_id, tenant_id, assigned_at)
                    values (:taskId, :tagId, :tenantId, :assignedAt)
                    """, params);
        }
    }

    private void seedAssets(String tenantId, int size) {
        LocalDate baseDate = LocalDate.of(2025, 2, 1);
        for (int i = 1; i <= size; i++) {
            MapSqlParameterSource params = new MapSqlParameterSource()
                    .addValue("id", (long) i)
                    .addValue("tenantId", tenantId)
                    .addValue("projectId", (long) i)
                    .addValue("serialNumber", "ASSET-%06d".formatted(i))
                    .addValue("purchaseCost", BigDecimal.valueOf(250000L + (long) i * 20L).setScale(2))
                    .addValue("weightKg", 10.0d + (i % 40) * 0.25d)
                    .addValue("purchasedOn", baseDate.plusDays(i % 200))
                    .addValue("active", i % 7 != 0)
                    .addValue("metadata", "{\"assetIndex\":" + i + "}");
            jdbc.update("""
                    insert into rel_asset(
                        id, tenant_id, project_id, serial_number, purchase_cost, weight_kg,
                        purchased_on, active, metadata
                    )
                    values (
                        :id, :tenantId, :projectId, :serialNumber, :purchaseCost, :weightKg,
                        :purchasedOn, :active, :metadata
                    )
                    """, params);
        }
    }

    private void seedInvoices(String tenantId, int size) {
        Instant paidBase = Instant.parse("2025-02-01T00:00:00Z");
        LocalDate issueBase = LocalDate.of(2025, 1, 15);
        for (int i = 1; i <= size; i++) {
            boolean paid = i % 3 == 0;
            UUID externalUuid = UUID.nameUUIDFromBytes(("invoice-" + i).getBytes(StandardCharsets.UTF_8));
            MapSqlParameterSource params = new MapSqlParameterSource()
                    .addValue("id", (long) i)
                    .addValue("tenantId", tenantId)
                    .addValue("projectId", (long) i)
                    .addValue("invoiceNo", "INV-%06d".formatted(i))
                    .addValue("amount", BigDecimal.valueOf(75000L + (long) i * 15L).setScale(2))
                    .addValue("taxPercent", (short) ((i % 21) + 5))
                    .addValue("paid", paid)
                    .addValue("issuedOn", issueBase.plusDays(i % 180))
                    .addValue("paidAt", paid ? paidBase.plusSeconds(i * 480L) : null)
                    .addValue("externalUuid", externalUuid)
                    .addValue("note", "Invoice seed " + i);
            jdbc.update("""
                    insert into rel_invoice(
                        id, tenant_id, project_id, invoice_no, amount, tax_percent,
                        paid, issued_on, paid_at, external_uuid, note
                    )
                    values (
                        :id, :tenantId, :projectId, :invoiceNo, :amount, :taxPercent,
                        :paid, :issuedOn, :paidAt, :externalUuid, :note
                    )
                    """, params);
        }
    }

    private void seedProjectDependencies(String tenantId, int size) {
        Instant base = Instant.parse("2025-02-02T00:00:00Z");
        for (int i = 1; i <= size; i++) {
            long dependsOn = i == size ? 1L : i + 1L;
            MapSqlParameterSource params = new MapSqlParameterSource()
                    .addValue("projectId", (long) i)
                    .addValue("dependsOnProjectId", dependsOn)
                    .addValue("tenantId", tenantId)
                    .addValue("createdAt", base.plusSeconds(i * 600L));
            jdbc.update("""
                    insert into rel_project_dependency(project_id, depends_on_project_id, tenant_id, created_at)
                    values (:projectId, :dependsOnProjectId, :tenantId, :createdAt)
                    """, params);
        }
    }

    private void ensureDictionaryVersion(String tenantId, String dictCode, long version) {
        MapSqlParameterSource params = new MapSqlParameterSource()
                .addValue("tenantId", tenantId)
                .addValue("dictCode", dictCode)
                .addValue("version", version);
        int updated = jdbc.update("""
                update dictionary_meta
                set version = :version,
                    updated_at = CURRENT_TIMESTAMP
                where tenant_id = :tenantId
                  and dict_code = :dictCode
                """, params);
        if (updated > 0) {
            return;
        }
        jdbc.update("""
                insert into dictionary_meta(tenant_id, dict_code, version, last_source_revision, updated_at)
                values (:tenantId, :dictCode, :version, null, CURRENT_TIMESTAMP)
                """, params);
    }
}
