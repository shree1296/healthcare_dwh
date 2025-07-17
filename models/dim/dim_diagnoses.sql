-- models/dim/dim_diagnoses.sql

--{{ config(materialized='table') }}

select
    DIAGNOSIS_ID              as diagnosis_id,
    "diagnosis"               as diagnosis_name   -- quoted since it's lowercase staging)
from {{ ref('stg_diagnoses') }}
