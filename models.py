"""Modèles Pydantic pour la validation des données"""

from datetime import date
from typing import Optional
from uuid import UUID
from pydantic import BaseModel, validator

class Agent(BaseModel):
    id: UUID
    nom: str
    email: str
    telephone: str
    created_at: date

class Technicien(BaseModel):
    id: UUID
    nom: str
    email: str
    telephone: str
    created_at: date

class Client(BaseModel):
    id: UUID
    agent_id: UUID
    box_id: Optional[str]  # Nouveau champ pour l'ID de la box
    nom: str
    prenom: str
    email: str
    telephone: str
    adresse: str
    latitude: float
    longitude: float
    created_at: date

class Soumission(BaseModel):
    id: UUID
    client_id: UUID
    date_soumission: date
    statut: str

class Installation(BaseModel):
    id: UUID
    soumission_id: UUID
    date_planifiee: date
    date_realisation: Optional[date]
    date_appel: date

class Box(BaseModel):
    numero_serie: str
    client_id: UUID
    modele: str
    date_fabrication: date
    wifi_ssid: str

class Abonnement(BaseModel):
    id: UUID
    client_id: UUID
    forfait_id: int
    installation_id: UUID
    date_debut: date
    date_fin: date
    duree_renouvellement: int

class Paiement(BaseModel):
    id: UUID
    client_id: UUID
    abonnement_id: Optional[UUID]
    montant: int
    type_paiement: str
    date_paiement: date

class Feedback(BaseModel):
    id: UUID
    client_id: UUID
    installation_id: UUID
    satisfaction_produit: int
    note_techniciens: int
    commentaires: Optional[str]
    date_soumission: date