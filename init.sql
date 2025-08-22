-- Table des commerciaux
CREATE TABLE agents (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    nom VARCHAR(255) NOT NULL,
    email VARCHAR(255) NOT NULL UNIQUE,
    telephone VARCHAR(50) NOT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

-- Table des techniciens
CREATE TABLE techniciens (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    nom VARCHAR(255) NOT NULL,
    email VARCHAR(255) NOT NULL UNIQUE,
    telephone VARCHAR(50) NOT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

-- Table des clients
CREATE TABLE clients (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    agent_id UUID NOT NULL REFERENCES agents(id),
    box_id VARCHAR(50),
    nom VARCHAR(255) NOT NULL,
    prenom VARCHAR(255) NOT NULL,
    email VARCHAR(255) NOT NULL UNIQUE,
    telephone VARCHAR(50) NOT NULL,
    adresse TEXT NOT NULL,
    latitude DOUBLE PRECISION NOT NULL,
    longitude DOUBLE PRECISION NOT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

-- Table des forfaits
CREATE TABLE forfaits (
    id SERIAL PRIMARY KEY,
    nom VARCHAR(50) NOT NULL UNIQUE CHECK (nom IN ('50 Mbps', '200 Mbps')),
    prix_mensuel INTEGER NOT NULL CHECK (prix_mensuel IN (15000, 30000))
);

-- Insertion des forfaits initiaux
INSERT INTO forfaits (nom, prix_mensuel) VALUES 
('50 Mbps', 15000),
('200 Mbps', 30000);



-- Table des soumissions
CREATE TYPE statut_soumission AS ENUM ('soumis', 'planifié', 'complété');
CREATE TABLE soumissions (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    client_id UUID NOT NULL REFERENCES clients(id),
    date_soumission DATE NOT NULL,
    statut statut_soumission NOT NULL DEFAULT 'soumis'
);

-- Table des installations
CREATE TABLE installations (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    soumission_id UUID NOT NULL REFERENCES soumissions(id),
    date_planifiee DATE NOT NULL,
    date_realisation DATE,
    date_appel DATE NOT NULL
);


-- Table de liaison installations-techniciens
CREATE TABLE installation_techniciens (
    installation_id UUID NOT NULL REFERENCES installations(id),
    technicien_id UUID NOT NULL REFERENCES techniciens(id),
    PRIMARY KEY (installation_id, technicien_id)
);


-- Table des boxes
CREATE TABLE boxes (
    numero_serie VARCHAR(50) PRIMARY KEY,
    client_id UUID NOT NULL UNIQUE REFERENCES clients(id), -- UNIQUE constraint
    modele VARCHAR(100) NOT NULL,
    date_fabrication DATE NOT NULL,
    wifi_ssid VARCHAR(100) NOT NULL
);


-- Table des abonnements
CREATE TABLE abonnements (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    client_id UUID NOT NULL REFERENCES clients(id),
    forfait_id INTEGER NOT NULL REFERENCES forfaits(id),
    installation_id UUID NOT NULL REFERENCES installations(id),
    date_debut DATE NOT NULL,
    date_fin DATE NOT NULL,
    duree_renouvellement INTEGER NOT NULL CHECK (duree_renouvellement IN (1, 3, 6, 12))
);

-- Table des paiements
CREATE TABLE paiements (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    client_id UUID NOT NULL REFERENCES clients(id),
    abonnement_id UUID REFERENCES abonnements(id),
    montant INTEGER NOT NULL,
    type_paiement VARCHAR(50) NOT NULL CHECK (type_paiement IN ('initial', 'renouvellement')),
    date_paiement DATE NOT NULL
);


-- Table des feedbacks
CREATE TABLE feedback (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    client_id UUID NOT NULL REFERENCES clients(id),
    installation_id UUID NOT NULL REFERENCES installations(id),
    satisfaction_produit SMALLINT NOT NULL CHECK (satisfaction_produit BETWEEN 1 AND 5),
    note_techniciens SMALLINT NOT NULL CHECK (note_techniciens BETWEEN 1 AND 5),
    commentaires TEXT,
    date_soumission DATE NOT NULL
);



-- Index sur les forfaits
CREATE INDEX idx_forfaits_nom ON forfaits(nom);
CREATE INDEX idx_forfaits_prix ON forfaits(prix_mensuel);

-- Index sur les clients pour les recherches fréquentes
CREATE INDEX idx_clients_agent_id ON clients(agent_id);
CREATE INDEX idx_clients_box_id ON clients(box_id);
CREATE INDEX idx_clients_created_at ON clients(created_at);
CREATE INDEX idx_clients_email ON clients(email);
CREATE INDEX idx_clients_nom_prenom ON clients(nom, prenom);


-- Index sur les techniciens pour les recherches par date de création
CREATE INDEX idx_techniciens_created_at ON techniciens(created_at);
CREATE INDEX idx_techniciens_email ON techniciens(email);


-- Index sur les agents pour les recherches par date de création
CREATE INDEX idx_agents_created_at ON agents(created_at);
CREATE INDEX idx_agents_email ON agents(email);



-- Index sur les soumissions pour les requêtes fréquentes
CREATE INDEX idx_soumissions_client_id ON soumissions(client_id);
CREATE INDEX idx_soumissions_date ON soumissions(date_soumission);
CREATE INDEX idx_soumissions_statut ON soumissions(statut);
CREATE INDEX idx_soumissions_date_statut ON soumissions(date_soumission, statut);

-- Index sur les installations pour les requêtes fréquentes
CREATE INDEX idx_installations_soumission_id ON installations(soumission_id);
CREATE INDEX idx_installations_date_planifiee ON installations(date_planifiee);
CREATE INDEX idx_installations_date_realisation ON installations(date_realisation);
CREATE INDEX idx_installations_dates ON installations(date_planifiee, date_realisation);


-- Index sur la table de liaison
CREATE INDEX idx_installation_techniciens_inst ON installation_techniciens(installation_id);
CREATE INDEX idx_installation_techniciens_tech ON installation_techniciens(technicien_id);



-- Index sur les boxes pour les recherches fréquentes
CREATE INDEX idx_boxes_client_id ON boxes(client_id);
CREATE INDEX idx_boxes_modele ON boxes(modele);
CREATE INDEX idx_boxes_date_fabrication ON boxes(date_fabrication);


-- Index sur les abonnements pour les requêtes fréquentes
CREATE INDEX idx_abonnements_client_id ON abonnements(client_id);
CREATE INDEX idx_abonnements_forfait_id ON abonnements(forfait_id);
CREATE INDEX idx_abonnements_installation_id ON abonnements(installation_id);
CREATE INDEX idx_abonnements_date_debut ON abonnements(date_debut);
CREATE INDEX idx_abonnements_date_fin ON abonnements(date_fin);
CREATE INDEX idx_abonnements_dates ON abonnements(date_debut, date_fin);
CREATE INDEX idx_abonnements_actifs ON abonnements(date_debut, date_fin) WHERE date_debut <= CURRENT_DATE AND date_fin >= CURRENT_DATE;



-- Index sur les paiements pour les requêtes fréquentes
CREATE INDEX idx_paiements_client_id ON paiements(client_id);
CREATE INDEX idx_paiements_abonnement_id ON paiements(abonnement_id);
CREATE INDEX idx_paiements_date ON paiements(date_paiement);
CREATE INDEX idx_paiements_type ON paiements(type_paiement);
CREATE INDEX idx_paiements_date_type ON paiements(date_paiement, type_paiement);



-- Index sur les feedbacks pour les requêtes fréquentes
CREATE INDEX idx_feedback_client_id ON feedback(client_id);
CREATE INDEX idx_feedback_installation_id ON feedback(installation_id);
CREATE INDEX idx_feedback_date ON feedback(date_soumission);
CREATE INDEX idx_feedback_satisfaction ON feedback(satisfaction_produit);
CREATE INDEX idx_feedback_techniciens ON feedback(note_techniciens);

-- Index composés pour les requêtes analytiques courantes
CREATE INDEX idx_clients_created_at_agent ON clients(created_at, agent_id);
CREATE INDEX idx_installations_realisation_planifiee ON installations(date_realisation, date_planifiee);
CREATE INDEX idx_abonnements_client_dates ON abonnements(client_id, date_debut, date_fin);
CREATE INDEX idx_paiements_client_date ON paiements(client_id, date_paiement);