with source as(
    select * from {{ source('healthcare_db', 'diagnoses')}}
),
transformed as(
    select
        cast("diagnosis_id" as integer) as diagnosis_id,
        "diagnosis",
        "file_name",
        try_to_timestamp("loaded_at") as loaded_at,
        try_to_date("file_date") as file_date
    from source
)
select * from transformed