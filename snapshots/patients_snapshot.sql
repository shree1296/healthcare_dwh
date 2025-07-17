{% snapshot patients_snapshot %}
{{
    config(
        target_schema='staging',
        unique_key='patient_id',
        strategy='timestamp',
        updated_at='loaded_at'
    )
}}

select
    "patient_id"      as patient_id,
    "patient_name"    as patient_name,
    {{ standardize_gender('"gender"')}} as gender,
    "age"             as age,
    "city_id"         as city_id,
    "race"            as race,
    "file_name"       as file_name,
    "loaded_at"       as loaded_at,
    "file_date"       as file_date
from {{ source('healthcare_db', 'patients') }}

{% endsnapshot %}
