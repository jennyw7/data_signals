with impressions as (
    select * from {{ ref('stg_impressions') }}
),

campaigns as (
    select * from {{ ref('stg_campaigns') }}
)

select
    i.campaign_id,
    c.campaign_name,
    c.advertiser_id,
    i.event_date,
    count(*)                                        as impressions,
    sum(i.is_click::int)                            as clicks,
    sum(i.cost)                                     as spend,
    div0(sum(i.is_click::int), count(*))            as ctr,
    div0(sum(i.cost), count(*)) * 1000              as cpm
from impressions i
left join campaigns c using (campaign_id)
group by 1, 2, 3, 4
