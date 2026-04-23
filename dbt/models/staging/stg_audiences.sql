with source as (
    select * from {{ source('stackadapt_reporting', 'REDSHIFT') }}
)

select
    distinct
    audience_id,
    audience_name,
    segment_type
from source
where audience_id is not null
