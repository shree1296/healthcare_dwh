{{ config(
    alias = 'stg_cities'
) }}

with source as (

    select * 
    from {{ source('healthcare_db', 'cities') }}

),

cleaned as (

    select
        cast("city_id" as integer)         as city_id,
        initcap("city")                    as city_name,
        initcap("state")                   as state_name,
        "file_name",
        try_to_timestamp("loaded_at")     as loaded_at,
        try_to_date("file_date")          as file_date

    from source

)

select * from cleaned
