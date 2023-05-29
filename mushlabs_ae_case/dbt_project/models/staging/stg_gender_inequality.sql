with gender_inequality as (
    select * from {{source('core','raw_gho_gender_inequality')}}
)

select * from gender_inequality