{{ config(materialized='table') }}

with fhv_data as (
    select *, 
        'FHV' as service_type 
    from {{ ref('stg_fhv') }}
),
dim_zones as (
    select * from {{ ref('dim_zones') }}
    where borough != 'Unknown'
)

select 
    fhv_data.tripid, 
    fhv_data.dispatching_base_num, 
    fhv_data.Affiliated_base_number,
    fhv_data.pickup_datetime,
    fhv_data.dropoff_datetime,
    pickup_zone.borough as pickup_borough, 
    pickup_zone.zone as pickup_zone, 
    fhv_data.dropoff_locationid,
    dropoff_zone.borough as dropoff_borough, 
    dropoff_zone.zone as dropoff_zone
from fhv_data
-- inner join dim_zones as pickup_zone
-- on fhv_data.pickup_locationid = pickup_zone.locationid
-- inner join dim_zones as dropoff_zone
-- on fhv_data.dropoff_locationid = dropoff_zone.locationid