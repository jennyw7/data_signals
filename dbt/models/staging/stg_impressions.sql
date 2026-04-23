with source as (
    select * from {{ source('stackadapt_reporting', 'REDSHIFT') }}
)

select
    impression_id,
    campaign_id,
    audience_id,
    (click = 1)::boolean        as is_click,
    cost::float                 as cost,
    event_date::date            as event_date
from source
where impression_id is not null
