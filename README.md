# DVF Pipeline — French Real Estate Data Analytics

##  Présentation du Projet
Ce projet est un pipeline ETL (Extract, Transform, Load) complet conçu pour une startup proptech. Il automatise le traitement des données **DVF** (Demandes de Valeurs Foncières) françaises, permettant d'analyser les tendances du marché immobilier parisien à partir de fichiers de données massifs.

### Stack Technique
- **Orchestration** : Apache Airflow 2.8.1 (TaskFlow API)
- **Data Lake** : HDFS (Hadoop 3.2.1) via WebHDFS
- **Data Warehouse** : PostgreSQL 15
- **Traitement** : Python / Pandas (Optimisé pour le Big Data)
- **Infrastructure** : Docker Compose

---

##  Correctifs et Optimisations (Journal de bord)

Pour rendre ce projet fonctionnel et stable dans un environnement Docker, les modifications suivantes ont été apportées :


### 2. Résolution des conflits de ports
Déplacement de l'UI Airflow du port 8080 vers le port **8082**.


### 3. Connexion Postgres "Zero-Conf"
- **Problème** : La tâche `verifier_sources` échouait car l'ID de connexion `dvf_postgres` n'existait pas dans Airflow.
- **Solution** : Injection de la connexion via la variable d'environnement `AIRFLOW_CONN_DVF_POSTGRES` dans le fichier compose. Le pipeline est fonctionnel dès le premier lancement sans configuration manuelle.

### 4. Optimisation de la mémoire (Chunking Pandas)
- **Problème** : Crash du worker Airflow (`SIGTERM` / OOM) lors de la lecture du fichier DVF complet (~4 millions de lignes). Le chargement total en RAM provoquait aussi des pannes DNS.
- **Solution** : Migration de la tâche `traiter_donnees` vers un mode de lecture par blocs de 100 000 lignes

---

##  Démarrage

1. **Lancer l'infrastructure** :
   ```bash
   docker compose up -d
   ```
2. **Accès Airflow UI** : [http://localhost:8082](http://localhost:8082) (admin / admin)
3. **Accès HDFS UI** : [http://localhost:9870](http://localhost:9870)

---

##  Architecture Data Flow

```text
[ data.gouv.fr ]  --(Download)--> [ /tmp/dvf.csv ]
                                          |
                                    (WebHDFS PUT)
                                          |
                                    [ HDFS (/data/dvf/raw) ]
                                          |
                                    (Pandas Chunks)
                                          |
                                    [ PostgreSQL (dvf_raw / agregats) ]
                                          |
                                    (Materialized View)
                                          |
                                    [ Statistics / Trends ]
```

##  Description des Tables
- `dvf_raw` : Données nettoyées et filtrées (appartements, Paris).
- `prix_m2_arrondissement` : Table pivot pour les calculs de moyennes et médianes.
- `dvf_qualite_runs` : Logs de validité des données pour chaque exécution.
- `stats_marche` : Analyse des variations des prix mois sur mois.
