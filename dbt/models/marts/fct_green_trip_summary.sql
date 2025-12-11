with trip_data as (

    select *
    from {{ ref('stg_green_tripdata') }}

),
aggregated as (

    select
        date_trunc(pickup_datetime, day) as trip_date,
        count(*) as total_trips,
        avg(trip_distance) as avg_distance,
        avg(total_amount) as avg_total_amount
    from trip_data
    group by 1
)

select * from aggregated