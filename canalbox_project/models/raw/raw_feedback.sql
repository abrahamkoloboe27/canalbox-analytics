{{ config(
    materialized='table',
    tags=['raw']
) }}


select
    id,
    client_id,
    installation_id,
    satisfaction_produit,
    note_techniciens,
    commentaires,
    date_soumission
from {{ source('canalbox', 'feedback') }}