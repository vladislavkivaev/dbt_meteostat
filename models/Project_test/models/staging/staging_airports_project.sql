with source as (
select * from {{ source('raw', 'raw_airports') }}
)

select
faa 
name, as airport_name,
city,
from source
where faa is not null