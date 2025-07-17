--{{ config(materialized='table') }}



select
    procedure_id,
    procedure_name
from {{ ref('stg_procedures') }}

