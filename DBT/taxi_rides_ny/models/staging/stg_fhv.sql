{{ config(materialized='view') }}

with tripdata as 
(
  select *
  from {{ source('staging','fhv_out') }}
)
select
    -- identifiers
    {{ dbt_utils.surrogate_key(['dispatching_base_num', 'pickup_datetime']) }} as tripid,
    dispatching_base_num,
    Affiliated_base_number,

    -- timestamps
    cast(pickup_datetime as timestamp) as pickup_datetime,
    cast(dropOff_datetime as timestamp) as dropoff_datetime

from tripdata

-- dbt build --m <model.sql> --var 'is_test_run: false'
{% if var('is_test_run', default=true) %}

  limit 100

{% endif %}
