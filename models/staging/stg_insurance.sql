-- models/staging/stg_insurance.sql

with source as (

    select * 
    from {{ source('healthcare_db', 'insurance') }}

),

renamed as (

    select
        cast("insurance_id" as integer)        as insurance_id,
        "insurance_provider",
        "file_name",
        try_to_timestamp("loaded_at")            as loaded_at,
        try_to_date("file_date")                 as file_date

    from source

)

select * from renamed
