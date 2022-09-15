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
comment on table spec.seed_cursors is 'Spec: Tracks progress of seed jobs.';

create index idx_seed_cursors_status ON spec.seed_cursors(status);

alter table spec.seed_cursors owner to spec;
