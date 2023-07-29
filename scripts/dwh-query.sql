select
 policy_uuid,
 property_id
from `postgres_pod_prod_public.policies`
where
  1=1
  and bound = true
  and policy_bound_date >= date_sub(current_date(), interval 3 month)
order by policy_bound_date desc;
