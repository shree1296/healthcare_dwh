-- models/staging/stg_procedures.sql

with source as (

    select * 
    from {{ source('healthcare_db', 'procedures') }}

),

renamed as (

    select
        cast("procedure_id" as integer)     as procedure_id,
        initcap("procedure")                as procedure_name,
        "file_name",
        try_to_timestamp("loaded_at")        as loaded_at,
        try_to_date("file_date")             as file_date

    from source

)

select * from renamed
