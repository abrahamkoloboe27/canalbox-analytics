{{ config(
    materialized='table',
    tags=['raw']
) }}


select
    id,
    client_id,
    abonnement_id,
    montant,
    type_paiement,
    date_paiement
from {{ source('canalbox', 'paiements') }}