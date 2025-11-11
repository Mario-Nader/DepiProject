{{ config(materialized='table') }}
SELECT
    Time AS traffic_time,
    Date AS traffic_date,
    [DayOfTheWeek] AS day_of_week,  -- <-- Use square brackets
    CarCount,
    BikeCount,
    BusCount,
    TruckCount,
    Total AS total_vehicles,
    [TrafficSituation] AS traffic_situation -- <-- Use square brackets
FROM
    {{ source('azure_sql_traffic', 'TrafficLog') }}