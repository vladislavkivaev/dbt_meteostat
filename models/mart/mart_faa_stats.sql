
WITH departures AS (
SELECT origin, 
COUNT (DISTINCT (dest)) AS nunique_to,
COUNT(sched_dep_time) AS dep_planned,
SUM(cancelled) AS dep_cancelled,
SUM(diverted) AS dep_diverted,
COUNT(sched_dep_time) - SUM(cancelled) AS sum_of_occured_departures,
COUNT (DISTINCT (tail_number)) AS unique_airplanes_number_from,
COUNT (DISTINCT (airline)) AS unique_airlines_number_from
FROM {{ref('prep_flights')}} 
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
FROM {{ref('prep_flights')}} 
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
)
SELECT ap.city, ap.country, ap.name, ts.*
FROM total_stats ts
JOIN {{ref('prep_airports')}} ap
ON ts.airport_code = ap.faa



/* Airport Stats
 
In a table mart_faa_stats.sql we want to see for each airport over all time:

- unique number of departures connections

- unique number of arrival connections

- how many flight were planned in total (departures & arrivals)

- how many flights were canceled in total (departures & arrivals)

- how many flights were diverted in total (departures & arrivals)

- how many flights actually occured in total (departures & arrivals)

- (optional) how many unique airplanes travelled on average

- (optional) how many unique airlines were in service on average

- add city, country and name of the airport 
*/


