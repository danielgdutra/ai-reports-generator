with reference_week_data as (
  select
  job_reference,
  'Past Week' as week,
  job_title,
  job_city,
  job_state,
  job_division,
  1 as total_jobs,
  sum(impressions) as impressions,
  sum(clicks) as clicks,
  sum(apply_starts) as apply_starts,
  sum(applies) as applies,
  sum(selling_price) as spent
from
  `tfy_warehouse.v_channel_flow`
where
  client_id=20069
and
    reference_date BETWEEN DATE_SUB('{start_date}', INTERVAL 7 DAY) AND DATE_SUB('{end_date}', INTERVAL 1 DAY)
and
  source_type in ('Indeed')
and
    job_title is not null
group by 1,2,3,4,5,6,7
),
comparison_week_data as (
  select
  job_reference,
  'Week Before' as week,
  job_title,
  job_city,
  job_state,
  job_division,
  1 as total_jobs,
  sum(impressions) as impressions,
  sum(clicks) as clicks,
  sum(apply_starts) as apply_starts,
  sum(applies) as applies,
  sum(selling_price) as spent
from
  `tfy_warehouse.v_channel_flow`
where
  client_id=20069
and
    reference_date BETWEEN DATE_SUB('{start_date}', INTERVAL 14 DAY) AND DATE_SUB('{end_date}', INTERVAL 7 DAY)
and
  source_type in ('Indeed')
and
    job_title is not null
group by 1,2,3,4,5,6,7
)

SELECT
rd.job_reference as `Display ID`,
rd.week as `Week`,
rd.job_title as `Job Title`,
rd.job_city as `Job City`,
rd.job_state as `Job State`,
rd.job_division as `Division`,
total_jobs as `Total Jobs`,
sum(rd.impressions) as `Impressions`,
sum(rd.clicks) as `Clicks`,
sum(rd.apply_starts) as `Apply Starts`,
sum(rd.applies) as `Applies`,
coalesce(safe_divide(sum(rd.applies),sum(rd.clicks)),0) as `Conversion`,
sum(rd.spent) as `Spent`,
coalesce(SAFE_DIVIDE(SUM(rd.clicks), SUM(rd.impressions)),0) AS CTR,
coalesce(SAFE_DIVIDE(SUM(rd.apply_starts), SUM(rd.clicks)),0) AS `Clicks to Apply Starts`,
coalesce(SAFE_DIVIDE(SUM(rd.applies), SUM(rd.clicks)),0) AS `Clicks to Applies`,
coalesce(SAFE_DIVIDE(SUM(rd.spent), SUM(rd.clicks)),0) AS CPC,
coalesce(SAFE_DIVIDE(SUM(rd.spent), SUM(rd.applies)),0) AS CPA
FROM
  reference_week_data rd

GROUP BY 1,2,3,4,5,6,7

UNION ALL


SELECT
cd.job_reference as `Display ID`,
cd.week as `Week`,
cd.job_title as `Job Title`,
cd.job_city as `Job City`,
cd.job_state as `Job State`,
cd.job_division as `Division`,
total_jobs as `Total Jobs`,
sum(cd.impressions) as `Impressions`,
sum(cd.clicks) as `Clicks`,
sum(cd.apply_starts) as `Apply Starts`,
sum(cd.applies) as `Applies`,
coalesce(safe_divide(sum(cd.applies),sum(cd.clicks)),0) as `Conversion`,
sum(cd.spent) as `Spent`,
coalesce(SAFE_DIVIDE(SUM(cd.clicks), SUM(cd.impressions)),0) AS CTR,
coalesce(SAFE_DIVIDE(SUM(cd.apply_starts), SUM(cd.clicks)),0) AS `Clicks to Apply Starts`,
coalesce(SAFE_DIVIDE(SUM(cd.applies), SUM(cd.clicks)),0) AS `Clicks to Applies`,
coalesce(SAFE_DIVIDE(SUM(cd.spent), SUM(cd.clicks)),0) AS CPC,
coalesce(SAFE_DIVIDE(SUM(cd.spent), SUM(cd.applies)),0) AS CPA
FROM
  comparison_week_data cd

GROUP BY 1,2,3,4,5,6,7