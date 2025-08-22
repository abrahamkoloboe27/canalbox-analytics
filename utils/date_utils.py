"""Utilitaires pour la gestion des dates et jours ouvrables"""

from datetime import date, timedelta

def is_business_day(d: date) -> bool:
    """
    Vérifie si une date est un jour ouvrable (lundi à vendredi)
    
    Args:
        d (date): Date à vérifier
        
    Returns:
        bool: True si c'est un jour ouvrable
    """
    return d.weekday() < 5  # Lundi=0, Dimanche=6

def add_business_days(start_date: date, days: int) -> date:
    """
    Ajoute un nombre de jours ouvrables à une date
    
    Args:
        start_date (date): Date de départ
        days (int): Nombre de jours ouvrables à ajouter
        
    Returns:
        date: Nouvelle date
    """
    current = start_date
    while days > 0:
        current += timedelta(days=1)
        if is_business_day(current):
            days -= 1
    return current

def business_days_between(start_date: date, end_date: date) -> int:
    """
    Calcule le nombre de jours ouvrables entre deux dates
    
    Args:
        start_date (date): Date de début
        end_date (date): Date de fin
        
    Returns:
        int: Nombre de jours ouvrables
    """
    if start_date > end_date:
        start_date, end_date = end_date, start_date
        
    days = 0
    current = start_date
    while current <= end_date:
        if is_business_day(current):
            days += 1
        current += timedelta(days=1)
    return days

def subtract_business_days(start_date: date, days: int) -> date:
    """
    Soustrait un nombre de jours ouvrables d'une date
    
    Args:
        start_date (date): Date de départ
        days (int): Nombre de jours ouvrables à soustraire
        
    Returns:
        date: Nouvelle date
    """
    current = start_date
    while days > 0:
        current -= timedelta(days=1)
        if is_business_day(current):
            days -= 1
    return current