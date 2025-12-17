{{
  config(
    materialized='incremental',
    unique_key='trip_date',
    partition_by={
      "field": "trip_date",
      "data_type": "date"
    }
  )
}}

with trip_data as (

    select *
    from {{ ref('stg_green_tripdata') }}

),
aggregated as (

    select
        date(pickup_datetime) as trip_date,
        count(*) as total_trips,
        avg(trip_distance) as avg_distance,
        avg(total_amount) as avg_total_amount
    from trip_data
    group by 1
)

select * from aggregated