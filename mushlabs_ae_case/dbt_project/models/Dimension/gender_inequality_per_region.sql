{{ config(materialized='table') }}
WITH
  raw_gho_country_source AS (
    SELECT
      CountryCode AS country_code,
      Country AS country_name,
      RegionCode AS region_code,
      Region AS region_name
    FROM {{ ref('stg_country') }}
  ),

  raw_gho_gender_inequality_source AS (
    SELECT
      CountryCode AS country_code,
      NumericValue as inequality_value,
    FROM {{ ref('stg_gender_inequality') }}
  ),

  -- Create a joined table with gender inequality per region
  gender_inequality_per_region AS (
    SELECT
      gi.country_code,
      gi.inequality_value,
      c.region_name
    FROM
      raw_gho_gender_inequality_source gi
      JOIN raw_gho_country_source c ON gi.country_code = c.country_code
  )

SELECT
  region_name,
  COUNT(*) AS inequality_count,
  AVG(inequality_value) AS average_inequality
FROM
  gender_inequality_per_region
GROUP BY
  region_name
