with source as (
  select * from {{ source('raw', 'fhv_tripdata') }}
),

renamed as (
  select
    -- identifiers
    cast(dispatching_base_num as string) as dispatching_base_num,
    {{ safe_cast('Affiliated_base_number', 'string') }} as affiliated_base_number,

    -- timestamps
    cast(pickup_datetime as timestamp) as pickup_datetime,
    cast(dropOff_datetime as timestamp) as dropoff_datetime,

    -- location ids (nullable in raw)
    {{ safe_cast('PUlocationID', 'integer') }} as pickup_location_id,
    {{ safe_cast('DOlocationID', 'integer') }} as dropoff_location_id,

    -- trip info
    {{ safe_cast('SR_Flag', 'numeric') }} as sr_flag
  from source
)

select * from renamed