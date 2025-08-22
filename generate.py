#!/usr/bin/env python3
"""
Script de génération de données synthétiques pour Canalbox
Usage: python generate.py --agents 20 --clients 500 --start-date 2023-01-01
"""

import argparse
import logging
import random
from datetime import date, timedelta
from uuid import UUID, uuid4
from typing import List, Dict, Tuple, Set
from collections import defaultdict

import psycopg2
from faker import Faker

# Configuration des logs
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("logs/generation.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger("data_generator")

# Import des modules du projet
from config.settings import DB_CONFIG, DEFAULT_PARAMS, MODELES_BOX, COTONOU_COORDS
from utils.date_utils import add_business_days, subtract_business_days, is_business_day
from utils.validators import validate_paiement
from models import Agent, Technicien, Client, Soumission, Installation, Box, Abonnement, Paiement, Feedback

def get_clients_per_month(base_date: date, target_date: date) -> float:
    """Calcule le multiplicateur de clients pour un mois donné"""
    # Calculer le nombre de mois entre la date de base et la date cible
    months_diff = (target_date.year - base_date.year) * 12 + (target_date.month - base_date.month)
    
    # Croissance progressive de 100% sur 24 mois
    growth_factor = 1 + (months_diff * 0.1)  # 10% de croissance par mois
    
    # Période de pointe : 25 du mois au 8 du mois suivant
    is_peak_period = False
    if target_date.day >= 25:
        is_peak_period = True
    elif target_date.day <= 8 and target_date > base_date:
        # Vérifier si c'est le mois suivant
        if target_date.month == (base_date.month % 12) + 1 or (target_date.month == 1 and base_date.month == 12):
            is_peak_period = True
    
    if is_peak_period:
        growth_factor *= 2.0  # 100% de boost pendant la période de pointe
    
    # Fluctuations aléatoires mensuelles (-30% à +50%)
    monthly_variation = random.uniform(0.7, 1.5)
    growth_factor *= monthly_variation
    
    # Assurer un minimum de 0.5x la base
    return max(0.5, growth_factor)

def generate_agents(n: int, start_date: date) -> List[Agent]:
    """Génère des commerciaux avec des dates de création aléatoires et évolution réaliste"""
    fake = Faker('fr_FR')
    agents = []
    
    # Date de fin pour la génération (aujourd'hui)
    end_date = date.today()
    
    # Générer des agents sur la période complète
    current_date = start_date
    agent_count = 0
    target_agents = n
    
    # Suivre les emails uniques
    used_emails: Set[str] = set()
    
    while current_date <= end_date:
        # Calculer le nombre d'agents à créer pour ce mois
        multiplier = get_clients_per_month(start_date, current_date)
        agents_this_month = max(1, int((target_agents / 12) * multiplier * 0.3))  # 30% de la cible
        
        # Générer les agents pour ce mois
        month_start = date(current_date.year, current_date.month, 1)
        if current_date.month == 12:
            month_end = date(current_date.year, 12, 31)
        else:
            month_end = date(current_date.year, current_date.month + 1, 1) - timedelta(days=1)
        
        for _ in range(agents_this_month):
            if agent_count >= target_agents * 2:  # Limite de sécurité
                break
                
            created_at = fake.date_between(start_date=month_start, end_date=month_end)
            
            # Générer un email unique
            email = fake.email()
            attempts = 0
            while email in used_emails and attempts < 100:
                email = fake.email()
                attempts += 1
            used_emails.add(email)
            
            agents.append(Agent(
                id=uuid4(),
                nom=fake.name(),
                email=email,
                telephone=fake.phone_number(),
                created_at=created_at
            ))
            agent_count += 1
        
        # Passer au mois suivant
        if current_date.month == 12:
            current_date = date(current_date.year + 1, 1, 1)
        else:
            current_date = date(current_date.year, current_date.month + 1, 1)
    
    logger.info(f"Générés {len(agents)} agents")
    return agents

def generate_techniciens(n: int, start_date: date) -> List[Technicien]:
    """Génère des techniciens avec des dates de création aléatoires et évolution réaliste"""
    fake = Faker('fr_FR')
    techniciens = []
    
    # Date de fin pour la génération (aujourd'hui)
    end_date = date.today()
    
    # Générer des techniciens sur la période complète
    current_date = start_date
    tech_count = 0
    target_techniciens = n
    
    # Suivre les emails uniques
    used_emails: Set[str] = set()
    
    while current_date <= end_date:
        # Calculer le nombre de techniciens à créer pour ce mois
        multiplier = get_clients_per_month(start_date, current_date)
        techs_this_month = max(1, int((target_techniciens / 12) * multiplier * 0.4))  # 40% de la cible
        
        # Générer les techniciens pour ce mois
        month_start = date(current_date.year, current_date.month, 1)
        if current_date.month == 12:
            month_end = date(current_date.year, 12, 31)
        else:
            month_end = date(current_date.year, current_date.month + 1, 1) - timedelta(days=1)
        
        for _ in range(techs_this_month):
            if tech_count >= target_techniciens * 2:  # Limite de sécurité
                break
                
            created_at = fake.date_between(start_date=month_start, end_date=month_end)
            
            # Générer un email unique
            email = fake.email()
            attempts = 0
            while email in used_emails and attempts < 100:
                email = fake.email()
                attempts += 1
            used_emails.add(email)
            
            techniciens.append(Technicien(
                id=uuid4(),
                nom=fake.name(),
                email=email,
                telephone=fake.phone_number(),
                created_at=created_at
            ))
            tech_count += 1
        
        # Passer au mois suivant
        if current_date.month == 12:
            current_date = date(current_date.year + 1, 1, 1)
        else:
            current_date = date(current_date.year, current_date.month + 1, 1)
    
    logger.info(f"Générés {len(techniciens)} techniciens")
    return techniciens

def generate_clients(n: int, agents: List[Agent], start_date: date) -> List[Client]:
    """Génère des clients avec évolution réaliste au fil du temps"""
    fake = Faker('fr_FR')
    clients = []
    
    # Date de fin pour la génération (aujourd'hui)
    end_date = date.today()
    
    # Suivre les emails uniques
    used_emails: Set[str] = set()
    
    # Générer des clients mois par mois
    current_date = start_date
    
    while current_date <= end_date:
        # Calculer le nombre de clients à créer pour ce mois
        multiplier = get_clients_per_month(start_date, current_date)
        clients_this_month = max(10, int((n / 12) * multiplier))
        
        # Générer les clients pour ce mois
        month_start = date(current_date.year, current_date.month, 1)
        if current_date.month == 12:
            month_end = date(current_date.year, 12, 31)
        else:
            month_end = date(current_date.year, current_date.month + 1, 1) - timedelta(days=1)
        
        # Répartition des clients dans le mois
        daily_clients = defaultdict(int)
        for _ in range(clients_this_month):
            client_date = fake.date_between(start_date=month_start, end_date=month_end)
            daily_clients[client_date] += 1
        
        # Générer les clients pour chaque jour
        for client_date, count in daily_clients.items():
            # Choisir des agents éligibles (créés avant cette date)
            eligible_agents = [a for a in agents if a.created_at <= client_date]
            if not eligible_agents:
                continue
            
            for _ in range(count):
                # Choisir un agent
                agent = random.choice(eligible_agents)
                
                # Générer des coordonnées autour de Cotonou
                lat = COTONOU_COORDS["latitude"] + random.uniform(-COTONOU_COORDS["radius"], COTONOU_COORDS["radius"])
                lon = COTONOU_COORDS["longitude"] + random.uniform(-COTONOU_COORDS["radius"], COTONOU_COORDS["radius"])
                
                # Générer un email unique
                email = fake.email()
                attempts = 0
                while email in used_emails and attempts < 100:
                    email = fake.email()
                    attempts += 1
                used_emails.add(email)
                
                clients.append(Client(
                    id=uuid4(),
                    agent_id=agent.id,
                    box_id=None,  # Sera rempli plus tard
                    nom=fake.last_name(),
                    prenom=fake.first_name(),
                    email=email,
                    telephone=fake.phone_number(),
                    adresse=fake.address().replace('\n', ', '),
                    latitude=lat,
                    longitude=lon,
                    created_at=client_date
                ))
        
        # Passer au mois suivant
        if current_date.month == 12:
            current_date = date(current_date.year + 1, 1, 1)
        else:
            current_date = date(current_date.year, current_date.month + 1, 1)
    
    logger.info(f"Générés {len(clients)} clients")
    return clients

def generate_soumissions(clients: List[Client]) -> List[Soumission]:
    """Génère des soumissions liées aux clients"""
    soumissions = []
    
    for client in clients:
        soumissions.append(Soumission(
            id=uuid4(),
            client_id=client.id,
            date_soumission=client.created_at,
            statut="soumis"
        ))
    
    logger.info(f"Générées {len(soumissions)} soumissions")
    return soumissions

def generate_installations(soumissions: List[Soumission], techniciens: List[Technicien]) -> Tuple[List[Installation], List[Dict]]:
    """Génère des installations avec dates planifiées et réelles"""
    installations = []
    installation_techniciens = []
    
    for soumission in soumissions:
        # Calculer les dates (2-7 jours ouvrables après soumission)
        days_to_install = random.randint(2, 7)
        date_planifiee = add_business_days(soumission.date_soumission, days_to_install)
        
        # Date d'appel : 1-2 jours ouvrables avant installation
        days_to_call = random.randint(1, 2)
        date_appel = subtract_business_days(date_planifiee, days_to_call)
        
        # 95% des installations réussies le jour planifié
        if random.random() < 0.95:
            date_realisation = date_planifiee
        else:
            # Report de 1 jour ouvrable en cas d'échec
            date_realisation = add_business_days(date_planifiee, 1)
        
        installation = Installation(
            id=uuid4(),
            soumission_id=soumission.id,
            date_planifiee=date_planifiee,
            date_realisation=date_realisation,
            date_appel=date_appel
        )
        installations.append(installation)
        
        # Assigner 2 techniciens créés avant la date de soumission
        eligible_techs = [t for t in techniciens if t.created_at <= soumission.date_soumission]
        if len(eligible_techs) >= 2:
            selected_techs = random.sample(eligible_techs, 2)
        elif len(eligible_techs) == 1:
            # Si un seul technicien éligible, l'ajouter et en choisir un autre
            selected_techs = [eligible_techs[0]]
            # Ajouter un technicien aléatoire parmi les autres
            other_techs = [t for t in techniciens if t != eligible_techs[0]]
            if other_techs:
                selected_techs.append(random.choice(other_techs))
        else:
            # Si aucun technicien n'est éligible, choisir 2 aléatoires
            selected_techs = random.sample(techniciens, min(2, len(techniciens))) if techniciens else []
        
        for tech in selected_techs:
            installation_techniciens.append({
                "installation_id": installation.id,
                "technicien_id": tech.id
            })
    
    logger.info(f"Générées {len(installations)} installations avec {len(installation_techniciens)} assignations techniciens")
    return installations, installation_techniciens

def generate_boxes(clients: List[Client]) -> Tuple[List[Box], List[Client]]:
    """Génère des boxes pour les clients et met à jour les clients avec box_id"""
    boxes = []
    used_serials: Set[str] = set()
    
    # Créer une copie des clients pour mise à jour
    updated_clients = []
    
    for client in clients:
        # Générer un numéro de série unique
        year = client.created_at.year
        serial = f"CBX-{random.choice('0123456789ABCDEF')}{random.choice('0123456789ABCDEF')}" \
                 f"{random.choice('0123456789ABCDEF')}{random.choice('0123456789ABCDEF')}-{year}"
        
        # S'assurer que le numéro de série est unique
        attempts = 0
        while serial in used_serials and attempts < 100:
            serial = f"CBX-{random.choice('0123456789ABCDEF')}{random.choice('0123456789ABCDEF')}" \
                     f"{random.choice('0123456789ABCDEF')}{random.choice('0123456789ABCDEF')}-{year}"
            attempts += 1
        used_serials.add(serial)
        
        # Date de fabrication : 1 mois à 1 an avant la création du client
        fabrication_date = client.created_at - timedelta(days=random.randint(30, 365))
        
        boxes.append(Box(
            numero_serie=serial,
            client_id=client.id,
            modele=random.choice(MODELES_BOX),
            date_fabrication=fabrication_date,
            wifi_ssid=f"Canalbox_{random.randint(1000, 9999)}"
        ))
        
        # Mettre à jour le client avec l'ID de la box
        updated_client = Client(
            id=client.id,
            agent_id=client.agent_id,
            box_id=serial,  # Ajout de l'ID de la box
            nom=client.nom,
            prenom=client.prenom,
            email=client.email,
            telephone=client.telephone,
            adresse=client.adresse,
            latitude=client.latitude,
            longitude=client.longitude,
            created_at=client.created_at
        )
        updated_clients.append(updated_client)
    
    logger.info(f"Générées {len(boxes)} boxes")
    return boxes, updated_clients

def generate_abonnements(clients: List[Client], installations: List[Installation], forfaits: List[Dict], client_soumissions: Dict) -> List[Abonnement]:
    """Génère des abonnements initiaux et renouvellements avec comportement client réaliste"""
    abonnements = []
    # Créer un mapping soumission_id -> installation
    installation_dict = {inst.soumission_id: inst for inst in installations}
    
    # Forfait de base (50 Mbps à 15k) - le plus populaire
    forfait_base = next(f for f in forfaits if f["prix_mensuel"] == 15000)
    
    for client in clients:
        # Trouver la soumission correspondante
        if client.id not in client_soumissions:
            continue
            
        soumission = client_soumissions[client.id]
        if soumission.id not in installation_dict:
            continue
            
        installation = installation_dict[soumission.id]
        if not installation.date_realisation:
            continue
            
        # Abonnement initial - toujours le forfait de base
        duree = 1  # Abonnement initial toujours pour 1 mois
        
        abonnement = Abonnement(
            id=uuid4(),
            client_id=client.id,
            forfait_id=forfait_base["id"],
            installation_id=installation.id,
            date_debut=installation.date_realisation,
            date_fin=installation.date_realisation + timedelta(days=30 * duree),
            duree_renouvellement=duree
        )
        abonnements.append(abonnement)
        
        # Générer des renouvellements réalistes
        current_date = abonnement.date_fin
        max_renewals = 12  # Max 12 renouvellements
        renewal_count = 0
        
        # 95% des clients se réabonnent immédiatement après l'installation
        should_renew = random.random() < 0.95
        
        while should_renew and renewal_count < max_renewals:
            # 80% des clients se réabonnent dans les 2 jours suivant l'expiration
            if random.random() < 0.8:
                # Réabonnement dans 0-2 jours
                days_delay = random.randint(0, 2)
                date_paiement = current_date + timedelta(days=days_delay)
            else:
                # Réabonnement plus tard (0-10 jours)
                days_delay = random.randint(3, 10)
                date_paiement = current_date + timedelta(days=days_delay)
            
            # Vérifier que la date de paiement est plausible (pas dans le futur trop lointain)
            if date_paiement > date.today() + timedelta(days=30):
                break
                
            # 80% du temps, le client choisit le forfait de base (15k)
            # 20% du temps, il peut choisir le forfait haut débit (30k)
            if random.random() < 0.8:
                forfait = forfait_base
            else:
                forfait = next(f for f in forfaits if f["id"] != forfait_base["id"])
            
            # Durée du renouvellement - généralement 1 mois
            if random.random() < 0.7:
                duree = 1  # 70% du temps
            elif random.random() < 0.9:
                duree = 3  # 20% du temps
            else:
                duree = random.choice([6, 12])  # 10% du temps
            
            abonnement = Abonnement(
                id=uuid4(),
                client_id=client.id,
                forfait_id=forfait["id"],
                installation_id=installation.id,
                date_debut=current_date,
                date_fin=current_date + timedelta(days=30 * duree),
                duree_renouvellement=duree
            )
            abonnements.append(abonnement)
            current_date = abonnement.date_fin
            renewal_count += 1
            
            # Après 3 mois, la probabilité de réabonnement diminue
            if renewal_count >= 3:
                # 70% de chance de continuer après 3 mois
                should_renew = random.random() < 0.7
            else:
                # 90% de chance de continuer avant 3 mois
                should_renew = random.random() < 0.9
        
        # 15% des clients qui se sont arrêtés reviennent après une pause
        if renewal_count > 0 and random.random() < 0.15:
            # Pause de 1 à 6 mois
            pause_months = random.randint(1, 6)
            pause_days = pause_months * 30
            comeback_date = current_date + timedelta(days=pause_days)
            
            # Vérifier que la date de retour est plausible
            if comeback_date <= date.today() + timedelta(days=30):
                # 90% du temps, ces clients reviennent avec le forfait de base
                if random.random() < 0.9:
                    forfait = forfait_base
                else:
                    forfait = next(f for f in forfaits if f["id"] != forfait_base["id"])
                
                # Durée du retour - généralement 1 mois
                if random.random() < 0.8:
                    duree = 1
                else:
                    duree = random.choice([3, 6])
                
                comeback_abo = Abonnement(
                    id=uuid4(),
                    client_id=client.id,
                    forfait_id=forfait["id"],
                    installation_id=installation.id,
                    date_debut=comeback_date,
                    date_fin=comeback_date + timedelta(days=30 * duree),
                    duree_renouvellement=duree
                )
                abonnements.append(comeback_abo)
    
    logger.info(f"Générés {len(abonnements)} abonnements")
    return abonnements

def generate_paiements(clients: List[Client], abonnements: List[Abonnement], forfaits: List[Dict]) -> List[Paiement]:
    """Génère les paiements correspondant aux abonnements"""
    paiements = []
    
    # Organiser les abonnements par client
    abonnement_dict = {}
    for abonnement in abonnements:
        if abonnement.client_id not in abonnement_dict:
            abonnement_dict[abonnement.client_id] = []
        abonnement_dict[abonnement.client_id].append(abonnement)
    
    # Créer un mapping forfait_id -> prix
    forfait_prix = {f["id"]: f["prix_mensuel"] for f in forfaits}
    
    for client in clients:
        abos = abonnement_dict.get(client.id, [])
        if not abos:
            continue
            
        # Trier les abonnements par date de début
        abos.sort(key=lambda x: x.date_debut)
        
        # Paiement initial (toujours le premier abonnement)
        initial_abo = abos[0]
        montant_initial = 25000  # 10k frais installation + 15k premier mois
        
        paiements.append(Paiement(
            id=uuid4(),
            client_id=client.id,
            abonnement_id=initial_abo.id,
            montant=montant_initial,
            type_paiement="initial",
            date_paiement=client.created_at
        ))
        
        # Paiements de renouvellement (abonnements suivants)
        for abonnement in abos[1:]:
            montant = forfait_prix[abonnement.forfait_id] * abonnement.duree_renouvellement
            
            # La date de paiement est généralement proche du début de l'abonnement
            # Pour les réabonnements rapides (0-2 jours), paiement = date_debut
            # Pour les autres, paiement peut être quelques jours avant
            if abonnement.date_debut <= abonnement.date_fin:  # Vérification de cohérence
                days_before = random.randint(0, 2)
                date_paiement = abonnement.date_debut - timedelta(days=days_before)
                
                # S'assurer que la date de paiement n'est pas avant la date de fin de l'abonnement précédent
                previous_abos = [a for a in abos if a.date_fin <= abonnement.date_debut]
                if previous_abos:
                    previous_abo = max(previous_abos, key=lambda x: x.date_fin)
                    if date_paiement < previous_abo.date_fin:
                        date_paiement = previous_abo.date_fin
                
                paiements.append(Paiement(
                    id=uuid4(),
                    client_id=client.id,
                    abonnement_id=abonnement.id,
                    montant=montant,
                    type_paiement="renouvellement",
                    date_paiement=date_paiement
                ))
    
    logger.info(f"Générés {len(paiements)} paiements")
    return paiements

def generate_feedback(installations: List[Installation]) -> List[Feedback]:
    """Génère des feedbacks après installation"""
    fake = Faker('fr_FR')
    feedbacks = []
    
    for installation in installations:
        if not installation.date_realisation:
            continue
            
        # 80% des installations ont un feedback
        if random.random() < 0.8:
            # Feedback 1-3 jours après l'installation
            days_after = random.randint(1, 3)
            date_soumission = add_business_days(installation.date_realisation, days_after)
            
            # Générer des notes avec une moyenne de 4.2
            satisfaction = min(5, max(1, int(random.gauss(4.2, 0.8))))
            note_tech = min(5, max(1, int(random.gauss(4.5, 0.7))))
            
            feedbacks.append(Feedback(
                id=uuid4(),
                client_id=installation.soumission_id,  # À corriger lors de l'insertion
                installation_id=installation.id,
                satisfaction_produit=satisfaction,
                note_techniciens=note_tech,
                commentaires=fake.text(max_nb_chars=200) if random.random() > 0.3 else None,
                date_soumission=date_soumission
            ))
    
    logger.info(f"Générés {len(feedbacks)} feedbacks")
    return feedbacks

def insert_data_to_db(conn, data_dict: Dict):
    """Insère les données générées dans la base de données"""
    cursor = conn.cursor()
    
    try:
        # Agents
        for agent in data_dict["agents"]:
            cursor.execute("""
                INSERT INTO agents (id, nom, email, telephone, created_at)
                VALUES (%s, %s, %s, %s, %s)
            """, (
                str(agent.id),
                agent.nom,
                agent.email,
                agent.telephone,
                agent.created_at
            ))
        
        # Techniciens
        for technicien in data_dict["techniciens"]:
            cursor.execute("""
                INSERT INTO techniciens (id, nom, email, telephone, created_at)
                VALUES (%s, %s, %s, %s, %s)
            """, (
                str(technicien.id),
                technicien.nom,
                technicien.email,
                technicien.telephone,
                technicien.created_at
            ))
        
        # Clients (avec box_id)
        for client in data_dict["clients"]:
            cursor.execute("""
                INSERT INTO clients (id, agent_id, box_id, nom, prenom, email, telephone, adresse, latitude, longitude, created_at)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, (
                str(client.id),
                str(client.agent_id),
                client.box_id,  # Peut être None
                client.nom,
                client.prenom,
                client.email,
                client.telephone,
                client.adresse,
                client.latitude,
                client.longitude,
                client.created_at
            ))
        
        # Soumissions
        for soumission in data_dict["soumissions"]:
            cursor.execute("""
                INSERT INTO soumissions (id, client_id, date_soumission, statut)
                VALUES (%s, %s, %s, %s)
            """, (
                str(soumission.id),
                str(soumission.client_id),
                soumission.date_soumission,
                soumission.statut
            ))
        
        # Installations
        for installation in data_dict["installations"]:
            cursor.execute("""
                INSERT INTO installations (id, soumission_id, date_planifiee, date_realisation, date_appel)
                VALUES (%s, %s, %s, %s, %s)
            """, (
                str(installation.id),
                str(installation.soumission_id),
                installation.date_planifiee,
                installation.date_realisation,
                installation.date_appel
            ))
        
        # Installation-Techniciens
        for rel in data_dict["installation_techniciens"]:
            cursor.execute("""
                INSERT INTO installation_techniciens (installation_id, technicien_id)
                VALUES (%s, %s)
            """, (
                str(rel["installation_id"]),
                str(rel["technicien_id"])
            ))
        
        # Boxes
        for box in data_dict["boxes"]:
            cursor.execute("""
                INSERT INTO boxes (numero_serie, client_id, modele, date_fabrication, wifi_ssid)
                VALUES (%s, %s, %s, %s, %s)
            """, (
                box.numero_serie,
                str(box.client_id),
                box.modele,
                box.date_fabrication,
                box.wifi_ssid
            ))
        
        # Abonnements
        for abonnement in data_dict["abonnements"]:
            cursor.execute("""
                INSERT INTO abonnements (id, client_id, forfait_id, installation_id, 
                                       date_debut, date_fin, duree_renouvellement)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
            """, (
                str(abonnement.id),
                str(abonnement.client_id),
                abonnement.forfait_id,
                str(abonnement.installation_id),
                abonnement.date_debut,
                abonnement.date_fin,
                abonnement.duree_renouvellement
            ))
        
        # Paiements
        for paiement in data_dict["paiements"]:
            cursor.execute("""
                INSERT INTO paiements (id, client_id, abonnement_id, montant, 
                                     type_paiement, date_paiement)
                VALUES (%s, %s, %s, %s, %s, %s)
            """, (
                str(paiement.id),
                str(paiement.client_id),
                str(paiement.abonnement_id) if paiement.abonnement_id else None,
                paiement.montant,
                paiement.type_paiement,
                paiement.date_paiement
            ))
        
        # Feedback (corriger le client_id)
        for fb in data_dict["feedback"]:
            # Trouver le client_id à partir de l'installation
            cursor.execute("""
                SELECT s.client_id 
                FROM installations i 
                JOIN soumissions s ON i.soumission_id = s.id 
                WHERE i.id = %s
            """, (str(fb.installation_id),))
            result = cursor.fetchone()
            if result:
                client_id = result[0]
                
                cursor.execute("""
                    INSERT INTO feedback (id, client_id, installation_id, satisfaction_produit,
                                        note_techniciens, commentaires, date_soumission)
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                """, (
                    str(fb.id),
                    str(client_id),
                    str(fb.installation_id),
                    fb.satisfaction_produit,
                    fb.note_techniciens,
                    fb.commentaires,
                    fb.date_soumission
                ))
        
        conn.commit()
        logger.info("Données insérées avec succès dans la base de données")
        
    except Exception as e:
        conn.rollback()
        logger.error(f"Erreur lors de l'insertion des données: {str(e)}")
        raise
    finally:
        cursor.close()

def get_forfaits_from_db(conn) -> List[Dict]:
    """Récupère les forfaits depuis la base de données"""
    cursor = conn.cursor()
    cursor.execute("SELECT id, prix_mensuel FROM forfaits")
    forfaits = [{"id": row[0], "prix_mensuel": row[1]} for row in cursor.fetchall()]
    cursor.close()
    return forfaits

def main():
    parser = argparse.ArgumentParser(description='Générateur de données pour Canalbox')
    parser.add_argument('--agents', type=int, default=DEFAULT_PARAMS["agents_count"], 
                        help='Nombre d\'agents à générer')
    parser.add_argument('--techniciens', type=int, default=DEFAULT_PARAMS["techniciens_count"], 
                        help='Nombre de techniciens à générer')
    parser.add_argument('--clients', type=int, default=DEFAULT_PARAMS["clients_count"], 
                        help='Nombre de clients à générer')
    parser.add_argument('--start-date', type=str, default=DEFAULT_PARAMS["start_date"], 
                        help='Date de début pour la génération (YYYY-MM-DD)')
    parser.add_argument('--db-host', type=str, default=DB_CONFIG["host"], help='Hôte de la base de données')
    parser.add_argument('--db-port', type=int, default=int(DB_CONFIG["port"]), help='Port de la base de données')
    parser.add_argument('--db-name', type=str, default=DB_CONFIG["dbname"], help='Nom de la base de données')
    parser.add_argument('--db-user', type=str, default=DB_CONFIG["user"], help='Utilisateur de la base de données')
    parser.add_argument('--db-password', type=str, default=DB_CONFIG["password"], help='Mot de passe de la base de données')
    
    args = parser.parse_args()
    
    # Configuration de la connexion
    db_config = {
        "host": args.db_host,
        "port": args.db_port,
        "dbname": args.db_name,
        "user": args.db_user,
        "password": args.db_password
    }
    
    start_date = date.fromisoformat(args.start_date)
    
    logger.info(f"Démarrage de la génération de données avec les paramètres :")
    logger.info(f"- Agents: {args.agents}")
    logger.info(f"- Techniciens: {args.techniciens}")
    logger.info(f"- Clients: {args.clients}")
    logger.info(f"- Date de début: {start_date}")
    
    try:
        # Connexion à la base de données
        conn = psycopg2.connect(**db_config)
        
        # Récupérer les forfaits depuis la base
        forfaits = get_forfaits_from_db(conn)
        logger.info(f"Forfaits récupérés: {len(forfaits)}")
        
        # Génération des données
        agents = generate_agents(args.agents, start_date)
        techniciens = generate_techniciens(args.techniciens, start_date)
        clients = generate_clients(args.clients, agents, start_date)
        soumissions = generate_soumissions(clients)
        
        # Créer un mapping client_id -> soumission pour l'utiliser dans les abonnements
        client_soumissions = {}
        for s in soumissions:
            client_soumissions[s.client_id] = s
            
        installations, installation_techniciens = generate_installations(soumissions, techniciens)
        boxes, updated_clients = generate_boxes(clients)  # Génère les boxes et met à jour les clients
        abonnements = generate_abonnements(updated_clients, installations, forfaits, client_soumissions)
        paiements = generate_paiements(updated_clients, abonnements, forfaits)
        feedback = generate_feedback(installations)
        
        # Préparation des données
        data = {
            "agents": agents,
            "techniciens": techniciens,
            "clients": updated_clients,  # Utiliser les clients mis à jour avec box_id
            "soumissions": soumissions,
            "installations": installations,
            "installation_techniciens": installation_techniciens,
            "boxes": boxes,
            "abonnements": abonnements,
            "paiements": paiements,
            "feedback": feedback
        }
        
        # Insertion dans la base
        logger.info("Insertion des données dans la base de données...")
        insert_data_to_db(conn, data)
        
        # Statistiques
        logger.info(f"Statistiques de génération :")
        logger.info(f"- Agents générés: {len(agents)}")
        logger.info(f"- Techniciens générés: {len(techniciens)}")
        logger.info(f"- Clients générés: {len(clients)}")
        logger.info(f"- Installations réussies: {sum(1 for i in installations if i.date_realisation)}")
        logger.info(f"- Abonnements initiaux: {sum(1 for a in abonnements if a.duree_renouvellement == 1)}")
        logger.info(f"- Renouvellements: {len(abonnements) - sum(1 for a in abonnements if a.duree_renouvellement == 1)}")
        logger.info(f"- Paiements générés: {len(paiements)}")
        logger.info(f"- Feedbacks générés: {len(feedback)}")
        logger.info(f"- Boxes générées: {len(boxes)}")
        
        # Vérification des dates
        if clients:
            min_date = min(c.created_at for c in clients)
            max_date = max(c.created_at for c in clients)
            logger.info(f"- Dates clients: {min_date} à {max_date}")
            
            # Distribution mensuelle des clients
            monthly_counts = defaultdict(int)
            for client in clients:
                month_key = f"{client.created_at.year}-{client.created_at.month:02d}"
                monthly_counts[month_key] += 1
            
            logger.info("- Distribution mensuelle des clients:")
            for month, count in sorted(monthly_counts.items()):
                logger.info(f"  {month}: {count} clients")
        
        conn.close()
        logger.info("Génération terminée avec succès !")
        
    except Exception as e:
        logger.exception(f"Erreur lors de la génération des données: {str(e)}")
        raise

if __name__ == "__main__":
    main()