-- models/dim/dim_insurance.sql
--{{ config(materialized='table') }}

select
    insurance_id,
    "insurance_provider",
    "file_name",
    loaded_at,
    file_date
from {{ ref('stg_insurance') }}
