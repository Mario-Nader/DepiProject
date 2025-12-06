{{
  config(
    materialized='incremental',
    unique_key='unique_row_id'
  )
}}

SELECT
    -- Create a unique ID for the incremental model (including batch_number for uniqueness)
    CONVERT(NVARCHAR(64), HASHBYTES('SHA2_256',
        CONCAT(CONVERT(VARCHAR, [Date], 23), ' ', CONVERT(VARCHAR, [Time], 8), ' ', 
              CarCount, ' ', BikeCount, ' ', batch_number)
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
    [TrafficSituation] AS traffic_situation,
    batch_number  

FROM {{ source('local_sql', 'TrafficLog') }}

{% if is_incremental() %}

  -- This 'WHERE' clause only runs on incremental runs (not the first run)
  -- It finds the newest batch number *already in this table*...
  -- ...and tells dbt to only get new batches from the source.
  WHERE CAST(batch_number AS INT) > (
    SELECT COALESCE(MAX(CAST(batch_number AS INT)), 0)
    FROM {{ this }}
  )
{% endif %}