-- spec.event_cursors definition

CREATE TABLE IF NOT EXISTS spec.event_cursors (
    name character varying NOT NULL,
    id character varying NOT NULL,
    nonce bigint NOT NULL,
    timestamp bigint NOT NULL,
    CONSTRAINT event_cursors_pkey PRIMARY KEY (name)
);
comment on table spec.event_cursors is 'Spec: Stores the last event seen for each subscribed event channel.';

ALTER table "spec".event_cursors OWNER TO spec;