-- models/temp_models/temp_cities.sql

{{ config(materialized = 'ephemeral', tags = 'temp') }}

select 'Pre-hook CTAS should have created temp_cities_temp' as status

