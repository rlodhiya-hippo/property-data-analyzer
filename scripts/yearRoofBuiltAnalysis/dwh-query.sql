select
  p.id,
  p.policy_number,
  p.policy_uuid,
  p.policy_bound_date,
  p.property_id,
  json_extract_scalar(p.policy_info, '$.quote.rater_params.insuranceScore') as quoteInsuranceScore,
  json_extract_scalar(p.policy_info, '$.risk_scoring_service_results.segmentation.segment') as customerSegment,
  json_extract_scalar(p.policy_info, '$.risk_scoring_service_results.segmentation.normalized_result') as customerSegmentNormalized,
from `postgres_pod_prod_public.policies` as p
where
  1=1
  and p.policy_uuid is not null
  and p.property_id is not null
  and p.bound = true
  and p.product = 'ho3'
  and p.policy_bound_date >= date_sub(current_date(), interval 1 year)
  and json_extract(policy_info, '$.risk_scoring_service_results.segmentation') is not null
order by policy_bound_date desc;
