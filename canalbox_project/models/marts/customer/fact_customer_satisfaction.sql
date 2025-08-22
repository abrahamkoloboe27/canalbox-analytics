{{ config(
    materialized='table',
    tags=['customer', 'marts']
) }}

with satisfaction_data as (
    select
        ff.feedback_ym as feedback_month,
        fc.geographic_zone,
        count(*) as total_feedbacks,
        count(case when ff.is_complete_feedback = true then 1 end) as complete_feedbacks,
        avg(ff.product_satisfaction) as avg_product_satisfaction,
        avg(ff.technician_rating) as avg_technician_rating,
        -- Satisfaction positive (4-5 étoiles)
        sum(case when ff.product_satisfaction >= 4 then 1 else 0 end) as positive_product_feedbacks,
        sum(case when ff.technician_rating >= 4 then 1 else 0 end) as positive_technician_feedbacks,
        -- Satisfaction neutre (3 étoiles)
        sum(case when ff.product_satisfaction = 3 then 1 else 0 end) as neutral_product_feedbacks,
        sum(case when ff.technician_rating = 3 then 1 else 0 end) as neutral_technician_feedbacks,
        -- Satisfaction négative (1-2 étoiles)
        sum(case when ff.product_satisfaction <= 2 then 1 else 0 end) as negative_product_feedbacks,
        sum(case when ff.technician_rating <= 2 then 1 else 0 end) as negative_technician_feedbacks,
        -- NPS calculation
        (sum(case when ff.product_satisfaction >= 4 then 1 else 0 end)::float - 
         sum(case when ff.product_satisfaction <= 2 then 1 else 0 end)::float) / 
         count(*) * 100 as nps_score
    from {{ ref('fact_feedback') }} ff
    left join {{ ref('dim_clients') }} fc on ff.client_id = fc.client_id
    group by 1, 2
),

satisfaction_metrics as (
    select
        sd.*,
        -- Taux de satisfaction produit
        case 
            when sd.total_feedbacks > 0 
            then (sd.positive_product_feedbacks::float / sd.total_feedbacks) * 100 
            else 0 
        end as product_satisfaction_rate_percent,
        -- Taux de satisfaction technicien
        case 
            when sd.total_feedbacks > 0 
            then (sd.positive_technician_feedbacks::float / sd.total_feedbacks) * 100 
            else 0 
        end as technician_satisfaction_rate_percent,
        -- Taux de feedback complet
        case 
            when sd.total_feedbacks > 0 
            then (sd.complete_feedbacks::float / sd.total_feedbacks) * 100 
            else 0 
        end as complete_feedback_rate_percent,
        -- Score de satisfaction global
        (sd.avg_product_satisfaction + sd.avg_technician_rating) / 2 as overall_satisfaction_score
    from satisfaction_data sd
),

trend_analysis as (
    select
        sm.*,
        -- Évolution du NPS
        sm.nps_score - 
        lag(sm.nps_score) over (partition by sm.geographic_zone order by sm.feedback_month) as nps_change,
        -- Évolution de la satisfaction globale
        sm.overall_satisfaction_score - 
        lag(sm.overall_satisfaction_score) over (partition by sm.geographic_zone order by sm.feedback_month) as satisfaction_change
    from satisfaction_metrics sm
)

select * from trend_analysis
order by feedback_month desc, geographic_zone