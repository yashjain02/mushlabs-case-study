-- models/gender_inequality_per_region.sql

-- Assuming the source table names are "raw_gho_country_source" and "raw_gho_gender_inequality_source"
-- Replace schema_name with the appropriate schema name

WITH
  raw_gho_country_source AS (
    SELECT
      Code,
      Country,
      "Region Code",
      Region
    FROM {{ ref('stg_country') }}
  ),

  raw_gho_gender_inequality_source AS (
    SELECT
      CountryCode AS country_code,
      NumericValue as inequality_value,
    FROM {{ ref('stg_gender_inequality') }}
  ),

  -- Create a normalized version of the country table
  normalized_country AS (
    SELECT
      Code AS country_code,
      Country AS country_name,
      "Region Code" AS region_code,
      Region AS region_name
    FROM
      raw_gho_country_source
  ),

  -- Create a joined table with gender inequality per region
  gender_inequality_per_region AS (
    SELECT
      gi.country_code,
      gi.inequality_value,
      c.region_name
    FROM
      raw_gho_gender_inequality_source gi
      JOIN normalized_country c ON gi.country_code = c.country_code
  )

SELECT
  region_name,
  COUNT(*) AS inequality_count,
  AVG(inequality_value) AS average_inequality
FROM
  gender_inequality_per_region
GROUP BY
  region_name
