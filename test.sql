-- Taux de renouvellement par mois
SELECT 
    DATE_TRUNC('month', date_fin) AS mois,
    COUNT(*) FILTER (WHERE EXISTS (
        SELECT 1 FROM abonnements a2 
        WHERE a2.client_id = a1.client_id 
        AND a2.date_debut = a1.date_fin
    )) * 100.0 / COUNT(*) AS taux_renouvellement
FROM abonnements a1
WHERE date_fin < CURRENT_DATE
GROUP BY 1
ORDER BY 1;

-- Délai moyen d'installation (jours ouvrables)
SELECT 
    AVG(business_days_between(date_soumission, date_realisation)) AS delai_moyen
FROM soumissions s
JOIN installations i ON s.id = i.soumission_id;

-- Satisfaction moyenne par modèle de box
SELECT 
    b.modele,
    AVG(f.satisfaction_produit) AS satisfaction_moyenne,
    AVG(f.note_techniciens) AS note_tech_moyenne
FROM feedback f
JOIN boxes b ON f.client_id = b.client_id
GROUP BY b.modele;