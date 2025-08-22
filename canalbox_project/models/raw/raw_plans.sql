{{ config(
    materialized='table',
    tags=['raw']
) }}

select
    id,
    nom,
    prix_mensuel
from {{ source('canalbox', 'forfaits') }}