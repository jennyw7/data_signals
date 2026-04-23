with impressions as (
    select * from {{ ref('stg_impressions') }}
),

audiences as (
    select * from {{ ref('stg_audiences') }}
)

select
    i.audience_id,
    a.audience_name,
    a.segment_type,
    i.event_date,
    count(*)                                    as impressions,
    sum(i.is_click::int)                        as clicks,
    sum(i.cost)                                 as spend,
    div0(sum(i.is_click::int), count(*))        as ctr
from impressions i
left join audiences a using (audience_id)
group by 1, 2, 3, 4
