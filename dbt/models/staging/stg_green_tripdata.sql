with source as (

    select *
    from {{ source('raw_data', 'green_tripdata_2021_01') }}

),
renamed as (

    select
        cast(VendorID as int) as vendor_id,
        cast(lpep_pickup_datetime as timestamp) as pickup_datetime,
        cast(lpep_dropoff_datetime as timestamp) as dropoff_datetime,
        store_and_fwd_flag,
        cast(RatecodeID as int) as rate_code_id,
        cast(PULocationID as int) as pu_location_id,
        cast(DOLocationID as int) as do_location_id,
        cast(passenger_count as int) as passenger_count,
        cast(trip_distance as float64) as trip_distance,
        cast(fare_amount as float64) as fare_amount,
        cast(extra as float64) as extra,
        cast(mta_tax as float64) as mta_tax,
        cast(tip_amount as float64) as tip_amount,
        cast(tolls_amount as float64) as tolls_amount,
        cast(ehail_fee as float64) as ehail_fee,
        cast(improvement_surcharge as float64) as improvement_surcharge,
        cast(total_amount as float64) as total_amount,
        cast(payment_type as int) as payment_type,
        cast(trip_type as int) as trip_type,
        cast(congestion_surcharge as float64) as congestion_surcharge
    from source

)
select * from renamed