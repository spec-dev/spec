-- Create spec schema

create schema if not exists spec AUTHORIZATION spec;

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