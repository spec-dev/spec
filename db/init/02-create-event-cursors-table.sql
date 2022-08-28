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