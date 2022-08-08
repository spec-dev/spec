-- Create spec schema and grant priveleges

create schema if not exists spec;

grant usage on schema spec to spec;
alter default privileges in schema spec grant all on tables to spec;
alter default privileges in schema spec grant all on functions to spec;
alter default privileges in schema spec grant all on sequences to spec;

grant usage on schema public to spec;
alter default privileges in schema public grant all on tables to spec;
alter default privileges in schema public grant all on functions to spec;
alter default privileges in schema public grant all on sequences to spec;