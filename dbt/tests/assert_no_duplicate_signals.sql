-- Custom test: no duplicate signals for the same campaign + week + signal_type
-- Fails if any duplicates exist in the signals layer

select
    campaign_id,
    week,
    signal_type,
    count(*) as cnt
from {{ ref('sig_ctr_drop') }}
group by 1, 2, 3
having count(*) > 1
