-- Signal: daily spend exceeds 2x the trailing 7-day average
-- Output: one row per campaign where this condition is true today

with daily as (
    select
        campaign_id,
        campaign_name,
        event_date,
        spend
    from {{ ref('mart_campaign_daily') }}
),

with_avg as (
    select
        *,
        avg(spend) over (
            partition by campaign_id
            order by event_date
            rows between 7 preceding and 1 preceding
        ) as avg_spend_7d
    from daily
)

select
    campaign_id,
    campaign_name,
    event_date,
    spend,
    avg_spend_7d,
    div0(spend, avg_spend_7d)   as spend_ratio,
    'spend_spike'               as signal_type
from with_avg
where
    avg_spend_7d is not null
    and avg_spend_7d > 0
    and div0(spend, avg_spend_7d) > 2.0
    and event_date = current_date()
