with country as (
    select * from {{source('core', 'raw_gho_countries')}}
)

select * from country