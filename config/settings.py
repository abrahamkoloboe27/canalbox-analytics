"""Configuration du projet Canalbox"""

# Configuration de la base de données
DB_CONFIG = {
    "dbname": "canalbox",
    "user": "canalbox",
    "password": "canalbox",
    "host": "localhost",
    "port": "5432"
}

# Paramètres de génération
DEFAULT_PARAMS = {
    "agents_count": 123,
    "techniciens_count": 86,
    "clients_count": 5729,
    "start_date": "2024-01-01"
}

# Modèles de box
MODELES_BOX = [
    "Huawei HG8245H",
    "ZTE F609", 
    "Nokia G-240W-A",
    "TP-Link Archer C5400",
    "D-Link DIR-882"
]

# Coordonnées de référence pour Cotonou, Bénin
COTONOU_COORDS = {
    "latitude": 6.3700,
    "longitude": 2.4324,
    "radius": 0.1  # Rayon de dispersion en degrés
}