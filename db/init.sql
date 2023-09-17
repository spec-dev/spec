--=======================================================
-- SPEC USER
--=======================================================
create user spec;

-- Create spec schema
create schema if not exists spec;
grant usage on schema spec to spec;
grant usage on schema public to spec;

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

--=======================================================
-- SPEC TRIGGER FUNCTIONS
--=======================================================

-- Table Subscriptions Function
CREATE OR REPLACE FUNCTION spec_table_sub() RETURNS trigger AS $$
DECLARE
    rec RECORD;
    base_payload TEXT;
    payload TEXT;
    column_name TEXT;
    column_value JSONB;
    primary_key_data JSONB;
    non_empty_columns TEXT;
    column_names_changed TEXT;
BEGIN
    -- Set record row depending on operation
    CASE TG_OP
    WHEN 'INSERT', 'UPDATE' THEN
        rec := NEW;
    WHEN 'DELETE' THEN
        rec := OLD;
    END CASE;

    -- Base event payload to publish.
    base_payload := ''
        || '{'
        || '"timestamp":"' || CURRENT_TIMESTAMP AT TIME ZONE 'UTC' || '",'
        || '"operation":"' || TG_OP                                || '",'
        || '"schema":"'    || TG_TABLE_SCHEMA                      || '",'
        || '"table":"'     || TG_TABLE_NAME                        || '",';

    -- For updates, add an array with the column names that changed.
    IF TG_OP = 'UPDATE' THEN
        column_names_changed := (SELECT json_agg(key)::text
            FROM json_each_text(to_json(OLD)) o
            JOIN json_each_text(to_json(rec)) n USING (key)
            WHERE n.value IS DISTINCT FROM o.value);
        base_payload := base_payload || '"columnNamesChanged":' || column_names_changed || ',';
    END IF;

    -- Try to publish the entire record inside the "data" key.
    BEGIN
        payload := base_payload || '"data":' || row_to_json(rec) || '}';
        PERFORM pg_notify('spec_data_change', payload);
        RETURN rec;

    -- Switch to reduced payload when record is too big (max 8kb).
    EXCEPTION
        WHEN others THEN            
            -- Build object with the names:values of the primary key columns (given to the function as an argument).
            IF TG_ARGV IS NOT NULL THEN
                FOREACH column_name IN ARRAY TG_ARGV LOOP
                    EXECUTE format('SELECT json_build_object(''%I'', $1.%I)', column_name, column_name)
                    INTO column_value
                    USING rec;
                    primary_key_data := coalesce(primary_key_data,'{}')::jsonb || column_value;
                END LOOP;
            ELSE
                primary_key_data := '{}'::jsonb;
            END IF;
            payload := base_payload || '"primaryKeyData":' || primary_key_data || '';
            
            -- For inserts, add an array with the column names that are not null.
            IF TG_OP = 'INSERT' THEN
                non_empty_columns := (SELECT json_agg(key)::text
                    FROM json_each_text(to_json(rec)) n
                    WHERE n.value IS NOT NULL);
                payload := payload || ',"nonEmptyColumns":' || non_empty_columns;
            END IF;
            payload := payload || '}';
            
            -- Publish (hopefully) smaller payload.
            BEGIN
                PERFORM pg_notify('spec_data_change', payload);
                RETURN rec;
            EXCEPTION
                WHEN others THEN
                    RETURN rec;
            END;
    END;
END;
$$ LANGUAGE plpgsql;

-- Op Tracking Function
CREATE OR REPLACE FUNCTION spec_track_ops() RETURNS trigger AS $$
DECLARE
    default_chain_id TEXT := TG_ARGV[0];
    chain_id_column_name TEXT := TG_ARGV[1];
    block_number_column_name TEXT := TG_ARGV[2];
    rec RECORD;
    rec_before JSON;
    rec_after JSON;
    block_number BIGINT;
    chain_id TEXT;
    block_number_floor BIGINT;
    pk_names_array TEXT[] := ARRAY[]::TEXT[];
    pk_names TEXT := '';
    pk_values_array TEXT[] := ARRAY[]::TEXT[];
    pk_values TEXT := '';
    pk_column_name TEXT;
    pk_column_value TEXT;
    insert_stmt TEXT;
    table_path TEXT;
    i INT = 0;
BEGIN
    -- Current table this trigger is actually on.
    table_path := TG_TABLE_SCHEMA || '.' || TG_TABLE_NAME;

    -- Get before/after record snapshots and block_number/chain_id.
    CASE TG_OP
    WHEN 'INSERT' THEN
        rec := NEW;
        rec_before := NULL;
        rec_after := row_to_json(NEW);
    WHEN 'UPDATE' THEN
        rec := NEW;
        rec_before := row_to_json(OLD);
        rec_after := row_to_json(NEW);
    WHEN 'DELETE' THEN
        rec := OLD;
        rec_before := row_to_json(OLD);
        rec_after := NULL;
    END CASE;

    -- Get block number from record.
    EXECUTE format('SELECT $1.%I', block_number_column_name)
    INTO block_number
    USING rec;

    -- Ensure block number exists.
    IF block_number IS NULL THEN
        RETURN rec;
    END IF;

    -- Get chain id from record (or default value).
    IF default_chain_id != '' THEN
        chain_id = default_chain_id;
    ELSE
        EXECUTE format('SELECT ($1.%I)::TEXT', chain_id_column_name)
        INTO chain_id
        USING rec;
    END IF;

    -- Ensure op-tracking is allowed for this block number.
    EXECUTE format('SELECT is_enabled_above from spec.op_tracking where table_path = $1 and chain_id = $2')
        INTO block_number_floor
        USING table_path, chain_id::TEXT;
    IF (block_number_floor IS NULL or block_number < block_number_floor) THEN
        RETURN rec;
    END IF;

    -- Curate a comma-delimited string of primary key values for the record.
    FOREACH pk_column_name IN ARRAY TG_ARGV LOOP
        i := i + 1;
        CONTINUE WHEN i < 4;
        EXECUTE format('SELECT ($1.%I)::TEXT', pk_column_name)
        INTO pk_column_value
        USING rec;
        pk_names_array := array_append(pk_names_array, pk_column_name::TEXT);
        pk_values_array := array_append(pk_values_array, pk_column_value::TEXT);
    END LOOP;
    pk_names := array_to_string(pk_names_array, ',');
    pk_values := array_to_string(pk_values_array, ',');

    -- Build and perform the ops table insert.
    EXECUTE format('INSERT INTO spec.ops ("table_path", "pk_names", "pk_values", "before", "after", "block_number", "chain_id") VALUES ($1, $2, $3, $4, $5, $6, $7)') 
    USING table_path, pk_names, pk_values, rec_before, rec_after, block_number, chain_id;

    RETURN rec;
END;
$$ LANGUAGE plpgsql;

--=======================================================
-- SPEC SCHEMA
--=======================================================

-- Event Cursors Table
create table if not exists spec.event_cursors (
    name character varying not null,
    id character varying not null,
    nonce character varying not null,
    "timestamp" timestamp with time zone not null,
    constraint event_cursors_pkey primary key (name)
);
comment on table spec.event_cursors is 'Spec: Stores the last event seen for each subscribed event channel.';
alter table spec.event_cursors owner to spec;

-- Live Columns Table
create table if not exists spec.live_columns (
    column_path character varying not null primary key,
    live_property character varying not null
);
comment on table spec.live_columns is 'Spec: Stores the current live columns.';
alter table spec.live_columns owner to spec;

-- Links Table
create table if not exists spec.links (
    table_path character varying not null,
    live_object_id character varying not null,
    unique_by character varying not null,
    filter_by character varying,
    constraint links_pkey primary key (table_path, live_object_id)
);
comment on table spec.links is 'Spec: Stores unique and filter by information.';
alter table spec.links owner to spec;

-- Table Sub Cursors Table
create table if not exists spec.table_sub_cursors (
    table_path character varying not null,
    "timestamp" timestamp with time zone not null,
    constraint table_sub_cursors_pkey primary key (table_path)
);
comment on table spec.table_sub_cursors is 'Spec: Stores the last table-sub event (table data-change event) seen for each live table.';
alter table spec.table_sub_cursors owner to spec;

-- Seed Cursors Table
create table if not exists spec.seed_cursors (
    id character varying not null primary key,
    job_type character varying not null,
    spec json not null,
    status character varying not null,
    cursor integer not null,
    metadata json,
    created_at timestamp with time zone not null
);
comment on table spec.seed_cursors is 'Spec: Tracks progress of seed jobs.';
create index idx_seed_cursors_status ON spec.seed_cursors(status);
alter table spec.seed_cursors owner to spec;
create trigger on_insert_seed_cursors after insert on spec.seed_cursors for each row execute function spec_table_sub('id');
create trigger on_update_seed_cursors after update on spec.seed_cursors for each row execute function spec_table_sub('id');
create trigger on_delete_seed_cursors after delete on spec.seed_cursors for each row execute function spec_table_sub('id');

-- Migrations Table
create table if not exists spec.migrations (
    "version" character varying not null primary key
);
comment on table spec.migrations is 'Spec: Stores the latest schema migration version.';
alter table spec.migrations owner to spec;

-- Ops Table
create table if not exists spec.ops (
    id serial primary key,
    table_path varchar not null,
    pk_names text not null,
    pk_values text not null,
    "before" json,
    "after" json,
    block_number bigint not null,
    chain_id varchar not null,
    ts timestamp with time zone not null default(now() at time zone 'utc')
);
comment on table spec.ops is 'Spec: Stores before & after snapshots of records at specific block numbers.';
create index idx_ops_table_pk_values on spec.ops(table_path, pk_values);
create index idx_ops_snapshot on spec.ops(block_number, chain_id);
create index idx_ops_table_snapshot on spec.ops(table_path, block_number, chain_id);
create index idx_ops_ordered on spec.ops(table_path, pk_values, block_number, ts);
alter table spec.ops owner to spec;

-- Op Tracking Table
create table if not exists spec.op_tracking (
    id serial primary key,
    table_path varchar not null,
    chain_id varchar not null,
    is_enabled_above bigint not null
);
comment on table spec.ops is 'Spec: Specifies whether ops should be tracked for a given table.';
create unique index idx_op_tracking_table_chain on spec.op_tracking(table_path, chain_id); 
alter table spec.op_tracking owner to spec;

-- Frozen Tables
create table if not exists spec.frozen_tables (
    id serial primary key,
    table_path varchar not null,
    chain_id varchar not null
);
comment on table spec.frozen_tables is 'Spec: Live tables actively ignoring new updates.';
create unique index idx_frozen_table_chain on spec.frozen_tables(table_path, chain_id); 
create index idx_frozen_tables_by_chain on spec.frozen_tables(chain_id); 
alter table spec.frozen_tables owner to spec;