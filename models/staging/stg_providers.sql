-- models/staging/stg_providers.sql

with source as (

    select * 
    from {{ source('healthcare_db', 'providers') }}

),

renamed as (

    select
        cast("provider_id" as integer)      as provider_id,
        initcap("provider_name")            as provider_name,
        lower("gender")                     as gender,
        initcap("nationality")              as nationality,
        cast("age" as integer)              as age,
        "image"                              as image_url,
        "file_name",
        try_to_timestamp("loaded_at")        as loaded_at,
        try_to_date("file_date")             as file_date

    from source

)

select * from renamed
