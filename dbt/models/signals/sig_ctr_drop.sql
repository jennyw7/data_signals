-- Signal: CTR dropped >10% compared to the previous week
-- Output: one row per campaign per week where this condition is true

with weekly as (
    select
        campaign_id,
        campaign_name,
        date_trunc('week', event_date)          as week,
        sum(clicks)                             as clicks,
        sum(impressions)                        as impressions,
        div0(sum(clicks), sum(impressions))     as ctr
    from {{ ref('mart_campaign_daily') }}
    group by 1, 2, 3
),

with_prev as (
    select
        *,
        lag(ctr) over (
            partition by campaign_id
            order by week
        ) as prev_ctr
    from weekly
)

select
    campaign_id,
    campaign_name,
    week,
    ctr,
    prev_ctr,
    {{ pct_change('ctr', 'prev_ctr') }}         as pct_change,
    'ctr_drop'                                  as signal_type
from with_prev
where
    prev_ctr is not null
    and prev_ctr > 0
    and {{ pct_change('ctr', 'prev_ctr') }} < -0.10
    and week = date_trunc('week', current_date())
