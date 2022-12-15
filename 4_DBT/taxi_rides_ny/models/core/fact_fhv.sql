{{ config(materialized='table') }}

WITH fhv_data AS (
    SELECT *, 
        'FHV' AS service_type 
    FROM {{ ref('stg_fhv') }}
),
dim_zones AS (
    SELECT * FROM {{ ref('dim_zones') }}
    where borough != 'Unknown'
)

SELECT 
    fhv_data.tripid, 
    fhv_data.dispatching_base_num, 
    fhv_data.Affiliated_base_number,
    fhv_data.pickup_datetime,
    fhv_data.dropoff_datetime,
    pickup_zone.borough AS pickup_borough, 
    pickup_zone.zone AS pickup_zone, 
    fhv_data.dropoff_locationid,
    dropoff_zone.borough AS dropoff_borough, 
    dropoff_zone.zone AS dropoff_zone
FROM fhv_data
-- INNER JOIN dim_zones AS pickup_zone
-- ON fhv_data.pickup_locationid = pickup_zone.locationid
-- INNER JOIN dim_zones AS dropoff_zone
-- ON fhv_data.dropoff_locationid = dropoff_zone.locationid
