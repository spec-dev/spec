-- spec.live_columns definition

create table if not exists spec.live_columns (
    column_path character varying not null,
    live_property character varying not null,
    seed_status character varying not null,
    constraint live_columns_pkey primary key (column_path)
);
comment on table spec.live_columns is 'Spec: Stores the current live columns.';

alter table "spec".live_columns owner to spec;