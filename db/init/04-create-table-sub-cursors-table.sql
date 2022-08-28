-- spec.table_sub_cursors definition

create table if not exists spec.table_sub_cursors (
    table_path character varying not null,
    "timestamp" timestamp with time zone not null,
    constraint table_sub_cursors_pkey primary key (table_path)
);
comment on table spec.table_sub_cursors is 'Spec: Stores the last table-sub event (table data-change event) seen for each live table.';

alter table spec.table_sub_cursors owner to spec;