-- Create spec user

create user spec;

-- Create spec schema

create schema if not exists spec;
grant usage on schema spec to spec;

-- Give spec user access to the "spec" schema

grant all privileges on all tables in schema spec to spec;
grant all privileges on all sequences in schema spec to spec;
grant all privileges on all functions in schema spec to spec;
alter default privileges in schema spec grant all on tables to spec;
alter default privileges in schema spec grant all on functions to spec;
alter default privileges in schema spec grant all on sequences to spec;

-- Give spec user access to the "public" schema

grant all privileges on all tables in schema public to spec;
grant all privileges on all sequences in schema public to spec;
grant all privileges on all functions in schema public to spec;
alter default privileges in schema public grant all on tables to spec;
alter default privileges in schema public grant all on functions to spec;
alter default privileges in schema public grant all on sequences to spec;

-- spec.event_cursors definition

create table if not exists spec.event_cursors (
    name character varying not null,
    id character varying not null,
    nonce bigint not null,
    "timestamp" timestamp with time zone not null,
    constraint event_cursors_pkey primary key (name)
);
comment on table spec.event_cursors is 'Spec: Stores the last event seen for each subscribed event channel.';

alter table spec.event_cursors owner to spec;

-- spec.live_columns definition

create table if not exists spec.live_columns (
    column_path character varying not null primary key,
    live_property character varying not null
);
comment on table spec.live_columns is 'Spec: Stores the current live columns.';

alter table spec.live_columns owner to spec;

-- spec.table_sub_cursors definition

create table if not exists spec.table_sub_cursors (
    table_path character varying not null,
    "timestamp" timestamp with time zone not null,
    constraint table_sub_cursors_pkey primary key (table_path)
);
comment on table spec.table_sub_cursors is 'Spec: Stores the last table-sub event (table data-change event) seen for each live table.';

alter table spec.table_sub_cursors owner to spec;

-- spec.seed_cursors definition

create table if not exists spec.seed_cursors (
    id character varying not null primary key,
    job_type character varying not null,
    spec json not null,
    status character varying not null,
    cursor integer not null,
    metadata json,
    created_at timestamp with time zone not null
);
comment on table spec.seed_cursors is 'Spec: Tracks progress of seed jobs for resiliency.';

create index idx_seed_cursors_status ON spec.seed_cursors(status);

alter table spec.seed_cursors owner to spec;