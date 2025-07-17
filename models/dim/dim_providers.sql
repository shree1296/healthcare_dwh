--{{ config(materialized='table') }}

-- models/dim/dim_providers.sql



select
    PROVIDER_ID            as provider_id,
    PROVIDER_NAME          as provider_name,
    GENDER,
    NATIONALITY,
    AGE,
    IMAGE_URL
from {{ ref('stg_providers') }}
