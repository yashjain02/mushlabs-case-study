-- models/gender_inequality_per_country.sql
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
      id,
      CountryCode AS country_code,
      DataCaptured AS data_captured_on,
      NumericValue,
      "Date",
      "TimeDimensionBegin",
      "TimeDimensionEnd"
    FROM {{ ref('stg_gender_inequality') }}
  ),

  -- Create a joined table with gender inequality per country
  gender_inequality_per_country AS (
    SELECT
      gi.id,
      gi.country_code,
      gi.data_captured_on,
      gi.NumericValue,
      gi.Date,
      gi."TimeDimensionBegin",
      gi."TimeDimensionEnd",
      c.country_name
    FROM
      raw_gho_gender_inequality_source gi
      JOIN raw_gho_country_source c ON gi.country_code = c.country_code
  )

SELECT
  country_name,
  COUNT(*) AS inequality_count,
  AVG(NumericValue) AS average_inequality
FROM
  gender_inequality_per_country
GROUP BY
  country_name
