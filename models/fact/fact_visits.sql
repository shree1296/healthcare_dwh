-- models/marts/fact_visits.sql

{{ config(
    materialized = 'incremental',
    unique_key = ['patient_id', 'visit_date'],
    on_schema_change = 'sync_all_columns'
) }}

with visits as (
    select * from {{ ref('stg_visits') }}
),

dim_patients as (
    select
        patient_id,
        gender,
        age,
        city_id
    from {{ ref('dim_patients') }}
),

dim_providers as (
    select
        provider_id,
        provider_name
    from {{ ref('dim_providers') }}
),

dim_departments as (
    select
        department_id,
        department_name
    from {{ ref('dim_departments') }}
),

dim_procedures as (
    select
        procedure_id,
        procedure_name
    from {{ ref('dim_procedures') }}
),

dim_diagnoses as (
    select
        diagnosis_id,
        diagnosis_name
    from {{ ref('dim_diagnoses') }}
),

dim_insurance as (
    select
        insurance_id,
        "insurance_provider"
    from {{ ref('dim_insurance') }}
)

select
    v.visit_date,
    v.patient_id,
    dp.gender,
    dp.age,
    dp.city_id,
    v.provider_id,
    pr.provider_name,
    v.department_id,
    dpt.department_name,
    v.diagnosis_id,
    dg.diagnosis_name,
    v.procedure_id,
    pc.procedure_name,
    v.insurance_id,
    ins."insurance_provider", 
    v."service_type",
    v.treatment_cost,
    v.medication_cost,
    v.follow_up_visit_date,
    v.patient_satisfaction_score,
    v."referral_source",
    v.emergency_visit_flag,
    v."payment_status",
    v.discharge_date,
    v.admitted_date,
    v."room_type",
    v.insurance_coverage_pct,
    v.room_daily_rate,
    v.loaded_at,
    v.file_date
from {{ ref('stg_visits') }} v
left join dim_patients dp      on v.patient_id     = dp.patient_id
left join dim_providers pr     on v.provider_id    = pr.provider_id
left join dim_departments dpt  on v.department_id  = dpt.department_id
left join dim_diagnoses dg     on v.diagnosis_id   = dg.diagnosis_id
left join dim_procedures pc    on v.procedure_id   = pc.procedure_id
left join dim_insurance ins    on v.insurance_id   = ins.insurance_id
