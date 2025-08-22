"""Validateurs pour les données métier Canalbox"""

from datetime import date, timedelta

def validate_installation_dates(
    date_soumission: date,
    date_planifiee: date,
    date_appel: date = None
) -> None:
    """
    Valide la cohérence des dates d'installation
    
    Args:
        date_soumission (date): Date de soumission
        date_planifiee (date): Date planifiée d'installation
        date_appel (date, optional): Date d'appel du client
        
    Raises:
        ValueError: Si les dates ne respectent pas les règles métier
    """
    # Vérifier que date_planifiee est 2-7 jours ouvrables après date_soumission
    business_days = business_days_between(date_soumission, date_planifiee)
    if business_days < 2 or business_days > 7:
        raise ValueError(
            f"La date planifiée ({date_planifiee}) doit être à 2-7 jours ouvrables "
            f"après la soumission ({date_soumission}), actuellement {business_days} jours"
        )
    
    # Vérifier date_appel si fournie
    if date_appel:
        business_days_call = business_days_between(date_appel, date_planifiee)
        if business_days_call < 1 or business_days_call > 2:
            raise ValueError(
                f"La date d'appel ({date_appel}) doit être à 1-2 jours ouvrables "
                f"avant l'installation ({date_planifiee}), actuellement {business_days_call} jours"
            )

def validate_abonnement_dates(
    date_debut: date,
    date_fin: date,
    duree_renouvellement: int
) -> None:
    """
    Valide que la durée de l'abonnement correspond à la durée de renouvellement
    
    Args:
        date_debut (date): Date de début de l'abonnement
        date_fin (date): Date de fin de l'abonnement
        duree_renouvellement (int): Durée de renouvellement en mois
        
    Raises:
        ValueError: Si les dates ne sont pas cohérentes
    """
    expected_days = duree_renouvellement * 30
    actual_days = (date_fin - date_debut).days
    
    # Tolérance de 1 jour pour les mois de 31 jours
    if abs(actual_days - expected_days) > 1:
        raise ValueError(
            f"La durée de l'abonnement ({actual_days} jours) ne correspond pas "
            f"à la durée de renouvellement ({duree_renouvellement} mois = {expected_days} jours)"
        )

def validate_paiement(montant: int, type_paiement: str, forfait_prix: int = None, duree: int = None) -> int:
    """
    Valide que le montant du paiement est correct
    
    Args:
        montant (int): Montant du paiement
        type_paiement (str): Type de paiement ('initial' ou 'renouvellement')
        forfait_prix (int, optional): Prix mensuel du forfait (pour renouvellement)
        duree (int, optional): Durée en mois (pour renouvellement)
        
    Returns:
        int: Montant validé
        
    Raises:
        ValueError: Si le montant n'est pas correct
    """
    if type_paiement == "initial":
        if montant != 25000:
            raise ValueError(
                f"Le paiement initial doit être de 25 000 XOF, pas {montant} XOF"
            )
        return 25000
    
    elif type_paiement == "renouvellement":
        if forfait_prix is None or duree is None:
            raise ValueError("Les paramètres forfait_prix et duree sont requis pour un renouvellement")
        
        expected = forfait_prix * duree
        
        if montant != expected:
            raise ValueError(
                f"Le renouvellement pour {duree} mois au forfait {forfait_prix} XOF/mois "
                f"doit être de {expected} XOF, pas {montant} XOF"
            )
        return expected
    
    else:
        raise ValueError(f"Type de paiement inconnu: {type_paiement}")

def business_days_between(start_date: date, end_date: date) -> int:
    """Helper function pour calculer les jours ouvrables"""
    if start_date > end_date:
        start_date, end_date = end_date, start_date
        
    days = 0
    current = start_date
    while current <= end_date:
        if current.weekday() < 5:  # Lundi à vendredi
            days += 1
        current += timedelta(days=1)
    return days