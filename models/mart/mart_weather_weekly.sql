SELECT airport_code,
DATE_TRUNC('week', date)::DATE AS week_start,
ROUND (AVG (avg_temp_c), 1) AS avg_weekly_temp_c,
ROUND (MIN (min_temp_c), 1) AS min_weekly_temp_c,
ROUND (MAX (max_temp_c), 1) AS max__weekly_temp_c,
ROUND (AVG (precipitation_mm), 1) AS avg_weekly_precipitation,
MAX (max_snow_mm) AS max_weekly_snow_mm,
ROUND (AVG (avg_wind_direction), 2) AS avg_weekly_wind_direction,
ROUND (AVG (avg_wind_speed), 1) AS avg_weekly_wind_speed,
ROUND (AVG (avg_peakgust), 1) AS avg_weekly_peakgust,
ROUND (AVG (avg_pressure_hpa), 1) AS avg_weekly_pressure_hpa,
ROUND (AVG (sun_minutes), 2) AS avg_weekly_sun_minutes
FROM {{ref('prep_weather_daily')}}
GROUP BY airport_code, week_start
ORDER BY week_start




/* 
 *
In a table mart_weather_weekly.sql we want to see all weather stats from the prep_weather_daily model aggregated weekly.

consider whether the metric should be Average, Maximum, Minimum, Sum or Mode

*/

