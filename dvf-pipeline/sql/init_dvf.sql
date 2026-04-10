-- Creation of the dvf database
-- Note: The docker-compose initializes with POSTGRES_DB=airflow
-- We create the dvf database here.
CREATE DATABASE dvf;

-- Switch to the dvf database
\c dvf

-- Table 1: dvf_raw
CREATE TABLE IF NOT EXISTS dvf_raw (
    id SERIAL PRIMARY KEY,
    date_mutation DATE,
    nature_mutation VARCHAR(50),
    valeur_fonciere NUMERIC(15,2),
    code_postal VARCHAR(10),
    nom_commune VARCHAR(100),
    type_local VARCHAR(50),
    surface_reelle_bati NUMERIC(10,2),
    nombre_pieces_principales INTEGER,
    created_at TIMESTAMP DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_dvf_commune ON dvf_raw(nom_commune);
CREATE INDEX IF NOT EXISTS idx_dvf_type ON dvf_raw(type_local);
CREATE INDEX IF NOT EXISTS idx_dvf_date ON dvf_raw(date_mutation);
CREATE INDEX IF NOT EXISTS idx_dvf_cp ON dvf_raw(code_postal);

-- Table 2: prix_m2_arrondissement
CREATE TABLE IF NOT EXISTS prix_m2_arrondissement (
    id SERIAL PRIMARY KEY,
    code_postal VARCHAR(10) NOT NULL,
    arrondissement INTEGER NOT NULL,
    annee INTEGER NOT NULL,
    mois INTEGER NOT NULL,
    prix_m2_moyen NUMERIC(10,2),
    prix_m2_median NUMERIC(10,2),
    prix_m2_min NUMERIC(10,2),
    prix_m2_max NUMERIC(10,2),
    nb_transactions INTEGER,
    surface_moyenne NUMERIC(10,2),
    updated_at TIMESTAMP DEFAULT NOW(),
    CONSTRAINT unique_agg_cp_date UNIQUE(code_postal, annee, mois)
);

-- Table 3: dvf_qualite_runs
CREATE TABLE IF NOT EXISTS dvf_qualite_runs (
    id               SERIAL PRIMARY KEY,
    date_run         TIMESTAMP DEFAULT NOW(),
    nb_total         INTEGER,
    nb_valides       INTEGER,
    taux_validite    FLOAT,
    nb_prix_aberrant INTEGER,
    nb_doublons      INTEGER
);

-- Table 4: stats_marche
CREATE TABLE IF NOT EXISTS stats_marche (
    id SERIAL PRIMARY KEY,
    arrondissement INTEGER,
    prix_courant NUMERIC(10,2),
    prix_precedent NUMERIC(10,2),
    variation_pct NUMERIC(6,2),
    annee INTEGER,
    mois INTEGER,
    created_at TIMESTAMP DEFAULT NOW()
);

-- Materialized View: dvf_evolution_mensuelle
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_matviews WHERE matviewname = 'dvf_evolution_mensuelle'
    ) THEN
        CREATE MATERIALIZED VIEW dvf_evolution_mensuelle AS
        SELECT
            code_postal,
            nom_commune,
            EXTRACT(YEAR FROM date_mutation)  AS annee,
            EXTRACT(MONTH FROM date_mutation) AS mois,
            COUNT(*)                          AS nb_transactions,
            ROUND(AVG(valeur_fonciere / NULLIF(surface_reelle_bati, 0))::numeric, 0) AS prix_moyen_m2,
            PERCENTILE_CONT(0.5) WITHIN GROUP (
                ORDER BY valeur_fonciere / NULLIF(surface_reelle_bati, 0)
            ) AS prix_median_m2
        FROM dvf_raw
        WHERE surface_reelle_bati > 0
          AND valeur_fonciere > 0
        GROUP BY code_postal, nom_commune,
                 EXTRACT(YEAR FROM date_mutation),
                 EXTRACT(MONTH FROM date_mutation)
        WITH DATA;
        CREATE INDEX ON dvf_evolution_mensuelle (code_postal, annee, mois);
    END IF;
END $$;
