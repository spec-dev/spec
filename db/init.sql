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