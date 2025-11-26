select * 
from {{ ref("stg_nascar__race") }}
where race_number not between -2 and 36