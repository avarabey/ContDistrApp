create table if not exists rel_org_unit (
    id bigint primary key,
    tenant_id varchar(128) not null,
    parent_id bigint null,
    unit_code char(8) not null,
    unit_name varchar(120) not null,
    active boolean not null default true,
    created_at timestamp with time zone not null,
    constraint uq_rel_org_unit_tenant_code unique (tenant_id, unit_code),
    constraint fk_rel_org_unit_parent foreign key (parent_id) references rel_org_unit(id)
);

create table if not exists rel_employee (
    id bigint primary key,
    tenant_id varchar(128) not null,
    org_unit_id bigint not null,
    manager_id bigint null,
    first_name varchar(80) not null,
    last_name varchar(80) not null,
    email varchar(160) not null,
    birth_date date not null,
    shift_start time not null,
    salary numeric(12,2) not null,
    bonus_rate double precision not null,
    full_time boolean not null,
    note text null,
    created_at timestamp with time zone not null,
    constraint uq_rel_employee_tenant_email unique (tenant_id, email),
    constraint fk_rel_employee_org_unit foreign key (org_unit_id) references rel_org_unit(id),
    constraint fk_rel_employee_manager foreign key (manager_id) references rel_employee(id)
);

create table if not exists rel_employee_profile (
    employee_id bigint primary key,
    tenant_id varchar(128) not null,
    profile_uuid uuid not null,
    preferred_shift smallint not null,
    emergency_phone varchar(32) not null,
    bio text null,
    reviewed_at timestamp with time zone not null,
    constraint uq_rel_employee_profile_uuid unique (profile_uuid),
    constraint chk_rel_employee_profile_shift check (preferred_shift between 1 and 3),
    constraint fk_rel_employee_profile_employee foreign key (employee_id) references rel_employee(id) on delete cascade
);

create table if not exists rel_project (
    id bigint primary key,
    tenant_id varchar(128) not null,
    org_unit_id bigint not null,
    lead_employee_id bigint not null,
    project_code varchar(32) not null,
    title varchar(160) not null,
    budget decimal(14,2) not null,
    start_date date not null,
    end_date date null,
    status char(1) not null,
    priority smallint not null,
    active boolean not null,
    created_at timestamp with time zone not null,
    constraint uq_rel_project_tenant_code unique (tenant_id, project_code),
    constraint chk_rel_project_status check (status in ('N', 'A', 'C')),
    constraint fk_rel_project_org_unit foreign key (org_unit_id) references rel_org_unit(id),
    constraint fk_rel_project_lead foreign key (lead_employee_id) references rel_employee(id)
);

create table if not exists rel_task (
    id bigint primary key,
    tenant_id varchar(128) not null,
    project_id bigint not null,
    assignee_employee_id bigint not null,
    task_key varchar(64) not null,
    title varchar(200) not null,
    details text null,
    estimate_hours integer not null,
    progress_percent smallint not null,
    due_at timestamp with time zone not null,
    completed boolean not null,
    payload_json text not null,
    deleted boolean not null default false,
    updated_at timestamp with time zone not null,
    constraint uq_rel_task_tenant_task_key unique (tenant_id, task_key),
    constraint chk_rel_task_progress check (progress_percent between 0 and 100),
    constraint fk_rel_task_project foreign key (project_id) references rel_project(id),
    constraint fk_rel_task_assignee foreign key (assignee_employee_id) references rel_employee(id)
);

create table if not exists rel_tag (
    id bigint primary key,
    tenant_id varchar(128) not null,
    tag_name varchar(64) not null,
    color_hex char(7) not null,
    system_tag boolean not null,
    created_on date not null,
    constraint uq_rel_tag_tenant_name unique (tenant_id, tag_name)
);

create table if not exists rel_task_tag (
    task_id bigint not null,
    tag_id bigint not null,
    tenant_id varchar(128) not null,
    assigned_at timestamp with time zone not null,
    primary key (task_id, tag_id),
    constraint fk_rel_task_tag_task foreign key (task_id) references rel_task(id) on delete cascade,
    constraint fk_rel_task_tag_tag foreign key (tag_id) references rel_tag(id) on delete cascade
);

create table if not exists rel_asset (
    id bigint primary key,
    tenant_id varchar(128) not null,
    project_id bigint not null,
    serial_number varchar(64) not null,
    purchase_cost numeric(12,2) not null,
    weight_kg double precision not null,
    purchased_on date not null,
    active boolean not null,
    metadata text null,
    constraint uq_rel_asset_project unique (project_id),
    constraint uq_rel_asset_tenant_serial unique (tenant_id, serial_number),
    constraint fk_rel_asset_project foreign key (project_id) references rel_project(id)
);

create table if not exists rel_invoice (
    id bigint primary key,
    tenant_id varchar(128) not null,
    project_id bigint not null,
    invoice_no varchar(64) not null,
    amount numeric(12,2) not null,
    tax_percent smallint not null,
    paid boolean not null,
    issued_on date not null,
    paid_at timestamp with time zone null,
    external_uuid uuid not null,
    note text null,
    constraint uq_rel_invoice_tenant_no unique (tenant_id, invoice_no),
    constraint uq_rel_invoice_uuid unique (external_uuid),
    constraint chk_rel_invoice_tax check (tax_percent between 0 and 100),
    constraint fk_rel_invoice_project foreign key (project_id) references rel_project(id)
);

create table if not exists rel_project_dependency (
    project_id bigint not null,
    depends_on_project_id bigint not null,
    tenant_id varchar(128) not null,
    created_at timestamp with time zone not null,
    primary key (project_id, depends_on_project_id),
    constraint chk_rel_project_dependency_self check (project_id <> depends_on_project_id),
    constraint fk_rel_project_dependency_project foreign key (project_id) references rel_project(id) on delete cascade,
    constraint fk_rel_project_dependency_depends_on foreign key (depends_on_project_id) references rel_project(id) on delete cascade
);
