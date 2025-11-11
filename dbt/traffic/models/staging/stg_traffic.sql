{{ config(materialized='incremental') }}

SELECT
    Time AS traffic_time,
    Date AS traffic_date,
    [DayOfTheWeek] AS day_of_week,
    CarCount,
    BikeCount,
    BusCount,
    TruckCount,
    Total AS total_vehicles,
    [TrafficSituation] AS traffic_situation
    -- (You will need a real timestamp column here)
FROM
    {{ source('azure_sql_traffic', 'TrafficLog') }}

{% if is_incremental() %}

  -- This 'WHERE' clause only runs on incremental runs
  -- It finds the max timestamp from *this* model (the target)
  -- and only selects *new* rows from the *source*
  WHERE timestamp_column > (SELECT max(timestamp_column) FROM {{ this }})

{% endif %}