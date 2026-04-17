WITH departures AS (
SELECT origin, flight_date,
COUNT (DISTINCT (dest)) AS nunique_to,
COUNT(sched_dep_time) AS dep_planned,
SUM(cancelled) AS dep_cancelled,
SUM(diverted) AS dep_diverted,
COUNT(sched_dep_time) - SUM(cancelled) AS sum_of_occured_departures,
COUNT (DISTINCT (tail_number)) AS unique_airplanes_number_from,
COUNT (DISTINCT (airline)) AS unique_airlines_number_from
FROM {{ref('prep_flights')}} 
GROUP BY origin, flight_date
),
arrivals AS (
SELECT dest, flight_date,
COUNT (DISTINCT (origin)) AS nunique_from,
COUNT (sched_arr_time) AS arr_planned,
SUM(cancelled) AS arr_cancelled,
SUM(diverted) AS arr_diverted,
COUNT(sched_arr_time) - SUM(cancelled) AS sum_of_occured_arrivals,
COUNT (DISTINCT (tail_number)) AS unique_airplanes_number_to,
COUNT (DISTINCT (airline)) AS unique_airlines_number_to
FROM {{ref('prep_flights')}}
GROUP BY dest, flight_date 
), 
total_stats AS (
SELECT d.origin AS airport_code, nunique_to, nunique_from, d.flight_date,
dep_planned + arr_planned AS total_planned, 
arr_cancelled + dep_cancelled AS total_cancelled, 
dep_diverted + arr_diverted AS total_diverted,
sum_of_occured_departures + sum_of_occured_arrivals AS total_occured_flights,
unique_airplanes_number_to, unique_airplanes_number_from, unique_airlines_number_to, unique_airlines_number_from
FROM departures d
JOIN arrivals a
ON d.origin = a.dest
AND d.flight_date = a.flight_date
),
mart_faa_stats AS (
SELECT ap.city, ap.country, ap.name, ts.*
FROM total_stats ts
JOIN {{ref('prep_airports')}} ap
ON ts.airport_code = ap.faa
)
SELECT mfs.airport_code,
pwd.date,
mfs.city,
mfs.country,
mfs.name,
mfs.nunique_to,
mfs.nunique_from,
mfs.total_planned,
mfs.total_cancelled,
mfs.total_diverted,
mfs.total_occured_flights,
mfs.unique_airplanes_number_to,
mfs.unique_airplanes_number_from,
mfs.unique_airlines_number_to,
mfs.unique_airlines_number_from,
pwd.min_temp_c AS daily_min_temperature,
pwd.max_temp_c AS daily_max_temperature,
pwd.precipitation_mm AS daily_precipitation_mm,
pwd.max_snow_mm AS daily_snowfall,
pwd.avg_wind_direction,
pwd.avg_wind_speed,
pwd.avg_peakgust AS avg_daily_peakgust
FROM mart_faa_stats mfs
JOIN {{ref('prep_weather_daily')}} pwd
ON mfs.airport_code = pwd.airport_code
AND mfs.flight_date = pwd.date
WHERE mfs.airport_code IN ('JFK', 'MIA', 'LAX')
ORDER BY date




-- Previous test version

/* SELECT *
FROM prep_airports;

WITH departures AS (
SELECT origin, 
COUNT (DISTINCT (dest)) AS nunique_to,
COUNT(sched_dep_time) AS dep_planned,
SUM(cancelled) AS dep_cancelled,
SUM(diverted) AS dep_diverted,
COUNT(sched_dep_time) - SUM(cancelled) AS sum_of_occured_departures,
COUNT (DISTINCT (tail_number)) AS unique_airplanes_number_from,
COUNT (DISTINCT (airline)) AS unique_airlines_number_from
FROM prep_flights 
GROUP BY origin
),
arrivals AS (
SELECT dest, 
COUNT (DISTINCT (origin)) AS nunique_from,
COUNT (sched_arr_time) AS arr_planned,
SUM(cancelled) AS arr_cancelled,
SUM(diverted) AS arr_diverted,
COUNT(sched_arr_time) - SUM(cancelled) AS sum_of_occured_arrivals,
COUNT (DISTINCT (tail_number)) AS unique_airplanes_number_to,
COUNT (DISTINCT (airline)) AS unique_airlines_number_to
FROM prep_flights
GROUP BY dest
), 
total_stats AS (
SELECT d.origin AS airport_code, nunique_to, nunique_from, 
dep_planned + arr_planned AS total_planned, 
arr_cancelled + dep_cancelled AS total_cancelled, 
dep_diverted + arr_diverted AS total_diverted,
sum_of_occured_departures + sum_of_occured_arrivals AS total_occured_flights,
unique_airplanes_number_to, unique_airplanes_number_from, unique_airlines_number_to, unique_airlines_number_from
FROM departures d
JOIN arrivals a
ON d.origin = a.dest
),
mart_faa_stats AS (
SELECT ap.city, ap.country, ap.name, ts.*
FROM total_stats ts
JOIN prep_airports ap
ON ts.airport_code = ap.faa
)
SELECT mfs.airport_code,
pwd.date,
mfs.city,
mfs.country,
mfs.name,
mfs.nunique_to,
mfs.nunique_from,
mfs.total_planned,
mfs.total_cancelled,
mfs.total_diverted,
mfs.total_occured_flights,
mfs.unique_airplanes_number_to,
mfs.unique_airplanes_number_from,
mfs.unique_airlines_number_to,
mfs.unique_airlines_number_from,
pwd.min_temp_c AS daily_min_temperature,
pwd.max_temp_c AS daily_max_temperature,
pwd.precipitation_mm AS daily_precipitation_mm,
pwd.max_snow_mm AS daily_snowfall,
pwd.avg_wind_direction,
pwd.avg_wind_speed,
pwd.avg_peakgust AS avg_daily_peakgust
FROM mart_faa_stats mfs
JOIN prep_weather_daily pwd
ON mfs.airport_code = pwd.airport_code
WHERE mfs.airport_code IN ('JFK', 'MIA', 'LAX')
ORDER BY date;

*/








/* In a table mart_selected_faa_stats_weather.sql we want to see for each airport daily:

only the airports we collected the weather data for
unique number of departures connections
unique number of arrival connections
how many flight were planned in total (departures & arrivals)
how many flights were canceled in total (departures & arrivals)
how many flights were diverted in total (departures & arrivals)
how many flights actually occured in total (departures & arrivals)
(optional) how many unique airplanes travelled on average
(optional) how many unique airlines were in service on average
(optional) add city, country and name of the airport
daily min temperature
daily max temperature
daily precipitation
daily snow fall
daily average wind direction
daily average wind speed
daily wnd peakgust 
*/