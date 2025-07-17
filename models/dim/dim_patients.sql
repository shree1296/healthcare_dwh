--{{ config(materialized='table') }}

select
    patient_id,
    patient_name,
    gender,
    age,
    city_id,
    race
from {{ ref('patients_snapshot') }}
where dbt_valid_to is null  -- ⏱ gets only current valid records
