{{ config(
    materialized = 'incremental',
    unique_key = ['patient_id', 'visit_date'],
    on_schema_change = 'sync_all_columns'
) }}

with source as (

    select * 
    from {{ source('healthcare_db', 'visits') }}

),

renamed as (

    select
        try_to_date("date_of_visit")                         as visit_date,
        cast("patient_id" as integer)                        as patient_id,
        cast("provider_id" as integer)                       as provider_id,
        cast("department_id" as integer)                     as department_id,
        cast("diagnosis_id" as integer)                      as diagnosis_id,
        cast("procedure_id" as integer)                      as procedure_id,
        cast("insurance_id" as integer)                      as insurance_id,
        "service_type",
        try_cast("treatment_cost" as float)                  as treatment_cost,
        try_cast("medication_cost" as float)                 as medication_cost,
        try_to_date("follow-up_visit_date")                  as follow_up_visit_date,
        cast("patient_satisfaction_score" as integer)        as patient_satisfaction_score,
        "referral_source",
        case 
            when lower("emergency_visit") in ('yes', 'true', '1') then true
            when lower("emergency_visit") in ('no', 'false', '0') then false
            else null 
        end      as emergency_visit_flag,
        "payment_status",
        try_to_date("discharge_date")                        as discharge_date,
        try_to_date("admitted_date")                         as admitted_date,
        "room_type",
        try_cast("insurance_coverage" as float)              as insurance_coverage_pct,
        try_cast("room_charges(daily_rate)" as float)        as room_daily_rate,
        "file_name",
        try_to_timestamp("loaded_at")                        as loaded_at,
        try_to_date("file_date")                             as file_date

    from source

),

--  Apply incremental filter using loaded_at
filtered as (

    select * from renamed

    {% if is_incremental() %}
      where loaded_at > (
        select max(loaded_at) from {{ this }}
      )
    {% endif %}

)

select * from filtered
