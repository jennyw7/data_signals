with impressions as (
    select * from {{ ref('stg_impressions') }}
),

campaigns as (
    select * from {{ ref('stg_campaigns') }}
),

audiences as (
    select * from {{ ref('stg_audiences') }}
)

select
    i.impression_id,
    i.campaign_id,
    c.campaign_name,
    i.audience_id,
    a.audience_name,
    a.segment_type,
    i.is_click,
    i.cost,
    i.event_date
from impressions i
left join campaigns c using (campaign_id)
left join audiences a using (audience_id)
