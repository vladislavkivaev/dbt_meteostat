with flights as (
    select * from {{ ref('staging_flights_project') }}
),

airports as (
    select * from {{ ref('staging_airports_project') }}
),

weather as (
    select * from {{ ref('staging_weather_project')}}
)

select
    f.flight_date,
    f.origin_airport,
    f.dest_airport,
    f.airline,
    f.flight_number,
    f.dep_delay,
    f.arr_delay,
    f.flight_cancelled,
    f.flight_diverted,
    f.distance,
    f.period_type,
    a.airport_name,
    a.city,
    w.date as weather_date,
    w.min_temp_c as min_daily_temperature,
    w.max_temp_c as max_daily_temperature,
    w.precipitation_mm as daily_precipitation_mm,
    w.max_snow_mm as max_daily_snow,
    w.avg_wind_direction as daily_avg_wind_direction,
    w.avg_wind_speed_kmh as daily_avg_wind_speed_kmh,
    w.wind_peakgust_kmh as daily_wind_peakgust_kmh,
    w.avg_pressure_hpa as daily_avg_pressure_hpa,
    w.sun_minutes as sun_minutes_per_day
from flights f
left join airports a
    on f.origin_airport = a.faa
left join weather w
    on f.origin_airport = w.airport_code