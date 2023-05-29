{{ config(materialized='table') }}
WITH gender_inequality_country AS (
  SELECT
    gi.id,
    gi.CountryCode,
    gi.DataCaptured,
    gi.NumericValue,
    gi.Date,
    gi.TimeDimensionBegin,
    gi.TimeDimensionEnd,
    c.Country,
    c.Region
  FROM
    {{ ref('stg_gender_inequality') }} gi
    JOIN {{ ref('stg_country') }} c ON gi.CountryCode = c.CountryCode
),

-- Step 2: Calculate the duration of gender inequality per country
gender_inequality_duration AS (
  SELECT
    CountryCode,
    DataCaptured,
    MIN(TimeDimensionBegin) AS min_begin,
    MAX(TimeDimensionEnd) AS max_end,
    (MAX(TimeDimensionEnd) - MIN(TimeDimensionBegin)) AS duration
  FROM
    gender_inequality_country
  GROUP BY
    CountryCode,
    DataCaptured
)

-- Step 3: Join with raw_gho_country to get country details
SELECT
  gid.CountryCode as country_code,
  gid.DataCaptured as year_data_collection_ended,
  gid.duration as duration_of_data,
  c.Country as country,
FROM
  gender_inequality_duration gid
  JOIN {{ ref('stg_country') }} c ON gid.CountryCode = c.CountryCode
