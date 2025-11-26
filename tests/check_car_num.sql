select * 
from {{ ref("stg_nascar__car") }}
where car_number not between 0 and 99