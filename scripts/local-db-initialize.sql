create extension if not exists "uuid-ossp";

drop index if exists policy_property_map_policy_id_idx;
drop index if exists policy_property_map_policy_uuid_idx;
drop index if exists policy_property_map_property_id_idx;
drop table if exists policy_property_map;
drop type if exists fetchStatus;

create type fetchStatus as enum ( 'pending', 'complete', 'failed' );

create table policy_property_map (
    id uuid primary key default uuid_generate_v4(),
    policy_id bigint not null,
    policy_number varchar(100) not null,
    policy_uuid uuid not null,
    application_uuid uuid not null,
    policy_bound_date date not null,
    property_id uuid not null,
    fetched fetchStatus not null default 'pending',
    policy_property_data jsonb null,
    property_data jsonb null
);

create index policy_property_map_policy_id_idx on policy_property_map (policy_id);

create index policy_property_map_policy_uuid_idx on policy_property_map (policy_uuid);

create index policy_property_map_property_id_idx on policy_property_map (property_id);
