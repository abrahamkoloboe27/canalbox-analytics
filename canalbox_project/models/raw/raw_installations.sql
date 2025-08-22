{{ config(
    materialized='table',
    tags=['raw']
) }}

select
    id,
    soumission_id,
    date_planifiee,
    date_realisation,
    date_appel
from {{ source('canalbox', 'installations') }}