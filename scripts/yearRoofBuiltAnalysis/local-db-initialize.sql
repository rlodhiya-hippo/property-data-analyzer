create extension if not exists "uuid-ossp";

drop index if exists policy_property_map_policy_uuid_idx;
drop index if exists policy_property_map_property_id_idx;
drop table if exists policy_property_map;
drop type if exists statusEnum;

create type statusEnum as enum ( 'blocked', 'pending', 'fetched', 'processed', 'complete', 'failed' );

create table policy_property_map (
    id uuid primary key default uuid_generate_v4(),
    policy_id integer not null,
    policy_number varchar(50) not null,
    policy_uuid uuid not null,
	policy_bound_date date not null,
    property_id uuid not null,
    quoteInsuranceScore integer not null,
    customer_segment varchar(50) not null,
    customer_segment_normalized varchar(50) not null,
    status statusEnum not null default 'pending',
    policy_data jsonb null,
    vendor_property_data jsonb null,
    property_data jsonb null,
    vendor_year_roof_built integer null,
    policy_year_roof_built integer null,
    diff_year_roof_built integer null,
    diff_vendor_year_roof_built integer null,
    vendor_roof_condition varchar(50) null
);

create index policy_property_map_policy_uuid_idx on policy_property_map (policy_uuid);

create index policy_property_map_property_id_idx on policy_property_map (property_id);
