with source as (
    select * from {{ source('raw', 'raw_flights') }}
)

select
    flight_date::date   as flight_date,
    origin              as origin_airport,
    dest                as dest_airport,
    airline,
    flight_number,
    dep_delay,
    arr_delay,
    cancelled           as is_cancelled,
    diverted            as is_diverted,
    distance
    period_type
from source
where flight_date is not null 