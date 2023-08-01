create extension if not exists "uuid-ossp";

drop index if exists policy_property_map_policy_uuid_idx;
drop index if exists policy_property_map_property_id_idx;
drop table if exists policy_property_map;
drop type if exists statusEnum;

create type statusEnum as enum ( 'pending', 'fetched', 'processed', 'complete', 'failed' );

create table policy_property_map (
    id uuid primary key default uuid_generate_v4(),
    policy_uuid uuid not null,
    property_id uuid not null,
    status statusEnum not null default 'pending',
    policy_data jsonb null,
    vendor_property_data jsonb null,
    property_data jsonb null,
    field_differences jsonb null,
    missing_policy_fields jsonb null
);

create index policy_property_map_policy_uuid_idx on policy_property_map (policy_uuid);

create index policy_property_map_property_id_idx on policy_property_map (property_id);

-- Reset fetched data in table
-- update policy_property_map
-- set
--     status = 'pending',
--     policy_data = null,
--     vendor_property_data = null,
--     property_data = null
-- where
--     status in ('fetched', 'processed', 'complete', 'failed');

-- Reset data for JSON compare
-- update policy_property_map
-- set
--     status = 'fetched',
--     field_differences = null,
--     missing_policy_fields = null
-- where
--     status in ('processed', 'complete', 'failed');

-- Reset data for Computing Diff Stats
-- update policy_property_map
-- set
--     status = 'processed'
-- where
--     status in ('complete');
