with source as (
        select * from {{ source('raw', 'fhv_tripdata') }}
  ),
  renamed as (
      select
          

      from source
  )
  select * from renamed
    