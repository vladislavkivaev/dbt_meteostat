WITH cte_1 AS (
SELECT CONCAT (origin, dest) AS route,
origin, 
dest, 
COUNT (*) AS total_flights,
COUNT (DISTINCT (tail_number)) AS unique_airplanes,
COUNT (DISTINCT (airline)) AS unique_airlines,
AVG (actual_elapsed_time) AS avg_actual_elapsed_time,
AVG (arr_delay_interval) AS average_arr_delay,
MAX (arr_delay_interval) AS max_delay,
MIN (arr_delay_interval) AS min_delay,
SUM(cancelled) AS totaL_number_of_cancelled,
SUM(diverted) AS total_number_diverted
FROM {{ref('prep_flights')}} 
GROUP BY route, origin, dest
)
SELECT pra.city AS origin_city, 
pra.country AS origin_country, 
pra.name origin_airport_name, 
prb.city AS destination_city, 
prb.country AS destination_country, 
prb.name AS destination_airport_name,
c.*
FROM cte_1 c
JOIN {{ref('prep_airports')}} pra
ON c.origin = pra.faa
JOIN {{ref('prep_airports')}} prb
ON c.dest = prb.faa







/* In a table mart_route_stats.sql we want to see for each route over all time:

origin airport code
destination airport code
total flights on this route
unique airplanes
unique airlines
on average what is the actual elapsed time
on average what is the delay on arrival
what was the max delay?
what was the min delay?
total number of cancelled
total number of diverted
add city, country and name for both, origin and destination, airports 
*/