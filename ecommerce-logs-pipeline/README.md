# Ecommerce Logs Pipeline

Ce projet implémente un pipeline d'ingestion de logs e-commerce vers un cluster HDFS en utilisant Apache Airflow pour l'orchestration.

## Architecture

Le projet s'appuie sur une infrastructure conteneurisée comprenant :
- Un cluster Airflow (Webserver, Scheduler, Postgres pour la base de données).
- Un cluster Hadoop HDFS (NameNode, DataNode).
- Un générateur de logs synthétiques au format Apache.

## Prérequis

- Docker et Docker Compose installés sur la machine hôte.

## Installation et lancement

1. Naviguer dans le répertoire du projet :
   cd ecommerce-logs-pipeline

2. Lancer les services en arrière-plan :
   docker compose up -d --build

3. Attendre l'initialisation complète de la base de données Airflow (environ 30 secondes).

## Accès aux interfaces

- Airflow Web UI : http://localhost:8081
  Identifiants : admin / admin
- HDFS Web UI : http://localhost:9870

## Structure du pipeline

Le DAG "logs_ecommerce_dag" effectue les opérations suivantes :
1. Création des répertoires HDFS requis.
2. Génération de logs journaliers locaux.
3. Upload des logs vers la zone "raw" de HDFS.
4. Analyse des codes de statut et statistiques.
5. Gestion d'alertes en cas de taux d'erreur élevé.
6. Archivage des logs traités vers la zone "processed".
