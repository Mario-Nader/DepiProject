{{
  config(
    materialized='incremental',
    unique_key='unique_row_id' -- (See note below about this key)
  )
}}

SELECT
    -- Create a unique ID for the incremental model
    CONVERT(NVARCHAR(64), HASHBYTES('SHA2_256',
        CONCAT(CONVERT(VARCHAR, [Date], 23), ' ', CONVERT(VARCHAR, [Time], 8), ' ', CarCount, ' ', BikeCount)
    ), 2) AS unique_row_id,

    -- Combine Date and Time into a real timestamp
    (CONVERT(DATETIME, [Date]) + CONVERT(DATETIME, [Time])) AS created_at,
    
    Time AS traffic_time,
    Date AS traffic_date,
    [DayOfTheWeek] AS day_of_week,
    CarCount,
    BikeCount,
    BusCount,
    TruckCount,
    Total AS total_vehicles,
    [TrafficSituation] AS traffic_situation

FROM
    {{ source('azure_sql_traffic', 'TrafficLog') }}

{% if is_incremental() %}

  -- This 'WHERE' clause only runs on incremental runs (not the first run)
  -- It finds the newest row *already in this table*...
  -- ...and tells dbt to only get new rows from the source.
  WHERE (CONVERT(DATETIME, [Date]) + CONVERT(DATETIME, [Time])) > (SELECT MAX(created_at) FROM {{ this }})

{% endif %}