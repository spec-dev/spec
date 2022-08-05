-- spec.live_columns definition

CREATE TABLE IF NOT EXISTS spec.live_columns (
    column_path character varying NOT NULL,
    live_property character varying NOT NULL,
    seed_status character varying NOT NULL,
    CONSTRAINT live_columns_pkey PRIMARY KEY (column_path)
);
comment on table spec.live_columns is 'Spec: Stores the current live columns.';

ALTER table "spec".live_columns OWNER TO spec;