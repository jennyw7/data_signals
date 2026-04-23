with source as (
    select * from {{ source('stackadapt_reporting', 'REDSHIFT') }}
)

select
    distinct
    campaign_id,
    campaign_name,
    advertiser_id
from source
where campaign_id is not null
