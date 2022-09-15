-- spec.live_columns definition

create table if not exists spec.live_columns (
    column_path character varying not null primary key,
    live_property character varying not null
);
comment on table spec.live_columns is 'Spec: Stores the current live columns.';

alter table spec.live_columns owner to spec;