select patient_id, count(*) as ct from {{ ref('stg_patients') }} group by patient_id
having ct > 1 