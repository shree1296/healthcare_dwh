with source as(
    select * from {{ source('healthcare_db', 'departments')}}
),

transformed as(
    select
        cast("department_id" as integer) as department_id,
        "department" as department_name,
        "file_name",
        try_to_timestamp("loaded_at") as loaded_at,
        try_to_date("file_date") as file_date
    from source 
)

select * from transformed