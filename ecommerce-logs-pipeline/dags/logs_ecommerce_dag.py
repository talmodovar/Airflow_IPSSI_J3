from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator, BranchPythonOperator
from datetime import datetime, timedelta
import subprocess
import logging
import os

default_args = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "email_on_failure": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
}

with DAG(
    dag_id="logs_ecommerce_dag",
    default_args=default_args,
    description="Pipeline d'ingestion de logs e-commerce vers HDFS",
    schedule_interval="@daily",
    start_date=datetime(2024, 3, 15),
    catchup=False,
    tags=["ecommerce", "hdfs", "logs"],
) as dag:

    # ── Tâche 1 : Créer les répertoires HDFS ──
    t_creer_repertoires = BashOperator(
        task_id="creer_repertoires_hdfs",
        bash_command="""
            echo "[INFO] Création des répertoires HDFS..."
            docker exec namenode hdfs dfs -mkdir -p /data/ecommerce/logs/raw
            docker exec namenode hdfs dfs -mkdir -p /data/ecommerce/logs/processed
            docker exec namenode hdfs dfs -mkdir -p /data/ecommerce/logs/curated
            docker exec namenode hdfs dfs -chmod -R 777 /data/ecommerce
            echo "[OK] Répertoires HDFS créés"
            docker exec namenode hdfs dfs -ls -R /data/ecommerce/
        """,
    )

    # ── Tâche 2 : Générer les logs journaliers ──
    def generer_logs_journaliers(**context):
        """
        Génère 1000 lignes de logs Apache pour la date d'exécution du DAG.
        """
        execution_date = context["ds"]
        fichier_sortie = f"/tmp/access_{execution_date}.log"
        script_path = "/opt/airflow/scripts/generer_logs.py"

        logging.info(f"Génération des logs pour {execution_date}...")
        result = subprocess.run(
            ["python3", script_path, execution_date, "1000", fichier_sortie],
            check=True,
            capture_output=True,
            text=True,
        )
        logging.info(result.stdout)

        if os.path.exists(fichier_sortie):
            taille = os.path.getsize(fichier_sortie)
            logging.info(f"Fichier généré : {fichier_sortie} ({taille} octets)")
        else:
            raise FileNotFoundError(f"Le fichier {fichier_sortie} n'a pas été créé")

        return fichier_sortie

    t_generer_logs = PythonOperator(
        task_id="generer_logs_journaliers",
        python_callable=generer_logs_journaliers,
    )

    # ── Tâche 3 : Uploader vers HDFS ──
    # On copie le fichier du conteneur airflow-scheduler vers le conteneur namenode,
    # puis on le met dans HDFS.
    t_uploader = BashOperator(
        task_id="uploader_vers_hdfs",
        bash_command="""
            EXECUTION_DATE="{{ ds }}"
            FICHIER_LOCAL="/tmp/access_${EXECUTION_DATE}.log"
            CHEMIN_HDFS="/data/ecommerce/logs/raw/access_${EXECUTION_DATE}.log"

            echo "[INFO] Copie du fichier local vers le conteneur namenode..."
            docker cp ${FICHIER_LOCAL} namenode:/tmp/access_${EXECUTION_DATE}.log

            echo "[INFO] Upload vers HDFS : ${CHEMIN_HDFS}"
            docker exec namenode hdfs dfs -put -f /tmp/access_${EXECUTION_DATE}.log ${CHEMIN_HDFS}

            echo "[INFO] Vérification..."
            docker exec namenode hdfs dfs -ls ${CHEMIN_HDFS}
            echo "[OK] Upload terminé"
        """,
    )

    # ── Tâche 4 : Vérifier le fichier dans HDFS ──
    # On utilise un BashOperator avec hdfs dfs -test pour vérifier l'existence du fichier
    t_verifier = BashOperator(
        task_id="verifier_fichier_hdfs",
        bash_command="""
            EXECUTION_DATE="{{ ds }}"
            CHEMIN_HDFS="/data/ecommerce/logs/raw/access_${EXECUTION_DATE}.log"

            echo "[INFO] Vérification de l'existence du fichier ${CHEMIN_HDFS}..."
            docker exec namenode hdfs dfs -test -e ${CHEMIN_HDFS}
            if [ $? -eq 0 ]; then
                echo "[OK] Le fichier existe dans HDFS"
                TAILLE=$(docker exec namenode hdfs dfs -du -s ${CHEMIN_HDFS} | awk '{print $1}')
                echo "[INFO] Taille du fichier : ${TAILLE} octets"
            else
                echo "[ERREUR] Le fichier n'existe pas dans HDFS !"
                exit 1
            fi
        """,
    )

    # ── Tâche 5 : Analyser les logs ──
    t_analyser = BashOperator(
        task_id="analyser_logs_hdfs",
        bash_command="""
            EXECUTION_DATE="{{ ds }}"
            CHEMIN_HDFS="/data/ecommerce/logs/raw/access_${EXECUTION_DATE}.log"

            echo "[INFO] Lecture du fichier HDFS : ${CHEMIN_HDFS}"
            docker exec namenode hdfs dfs -cat "${CHEMIN_HDFS}" > /tmp/logs_analyse_${EXECUTION_DATE}.txt

            echo "[INFO] Analyse des logs..."

            echo "=== STATUS CODES ==="
            grep -oP '"[A-Z]+ [^ ]+ HTTP/[0-9.]+" [0-9]+' /tmp/logs_analyse_${EXECUTION_DATE}.txt \
              | grep -oP '[0-9]+$' | sort | uniq -c | sort -rn

            echo "=== TOP 5 URLS ==="
            grep -oP '"(GET|POST) [^ ]+' /tmp/logs_analyse_${EXECUTION_DATE}.txt \
              | cut -d' ' -f2 | sort | uniq -c | sort -rn | head -5

            TOTAL=$(wc -l < /tmp/logs_analyse_${EXECUTION_DATE}.txt)
            ERREURS=$(grep -cP '"[A-Z]+ [^ ]+ HTTP/[0-9.]+" (4|5)[0-9]{2}' \
              /tmp/logs_analyse_${EXECUTION_DATE}.txt || echo 0)

            echo "=== TAUX ERREUR ==="
            echo "Total: ${TOTAL}, Erreurs: ${ERREURS}"

            echo "${ERREURS} ${TOTAL}" > /tmp/taux_erreur_${EXECUTION_DATE}.txt
        """,
    )

    # ── Tâche 6 : Décider si alerte nécessaire (BranchPythonOperator) ──
    def decider_alerte(**context):
        """
        Lit le taux d'erreur calculé par la tâche précédente.
        Si taux > 10% → alerter_equipe_ops
        Sinon → archiver_rapport_ok
        """
        execution_date = context["ds"]
        fichier_taux = f"/tmp/taux_erreur_{execution_date}.txt"

        with open(fichier_taux, "r") as f:
            contenu = f.read().strip().split()
            erreurs = int(contenu[0])
            total = int(contenu[1])

        taux = (erreurs / total * 100) if total > 0 else 0
        logging.info(f"Taux d'erreur : {taux:.2f}% ({erreurs}/{total})")

        if taux > 10:
            logging.warning(f"ALERTE : Taux d'erreur élevé ({taux:.2f}%)")
            return "alerter_equipe_ops"
        else:
            logging.info(f"Taux d'erreur normal ({taux:.2f}%)")
            return "archiver_rapport_ok"

    t_decider = BranchPythonOperator(
        task_id="decider_alerte",
        python_callable=decider_alerte,
    )

    # ── Tâche 7a : Alerter l'équipe Ops ──
    def alerter_equipe_ops(**context):
        execution_date = context["ds"]
        fichier_taux = f"/tmp/taux_erreur_{execution_date}.txt"
        with open(fichier_taux, "r") as f:
            contenu = f.read().strip().split()
            erreurs = int(contenu[0])
            total = int(contenu[1])
        taux = (erreurs / total * 100) if total > 0 else 0
        logging.warning(f"🚨 ALERTE OPS — {execution_date}")
        logging.warning(f"Taux d'erreur : {taux:.2f}% ({erreurs} erreurs sur {total} requêtes)")
        logging.warning("Action : Vérifier les serveurs web et les logs d'erreur 5xx")
        # En production : envoyer un email, Slack, PagerDuty, etc.

    t_alerter = PythonOperator(
        task_id="alerter_equipe_ops",
        python_callable=alerter_equipe_ops,
    )

    # ── Tâche 7b : Archiver rapport OK ──
    def archiver_rapport_ok(**context):
        execution_date = context["ds"]
        logging.info(f"✅ Rapport OK pour {execution_date} — Aucune alerte nécessaire")

    t_archive_ok = PythonOperator(
        task_id="archiver_rapport_ok",
        python_callable=archiver_rapport_ok,
    )

    # ── Tâche 8 : Archiver les logs (déplacer raw → processed) ──
    t_archiver = BashOperator(
        task_id="archiver_logs_hdfs",
        bash_command="""
            EXECUTION_DATE="{{ ds }}"
            SOURCE="/data/ecommerce/logs/raw/access_${EXECUTION_DATE}.log"
            DESTINATION="/data/ecommerce/logs/processed/access_${EXECUTION_DATE}.log"

            echo "[INFO] Déplacement HDFS : ${SOURCE} → ${DESTINATION}"
            docker exec namenode hdfs dfs -mv "${SOURCE}" "${DESTINATION}"
            echo "[OK] Fichier archivé dans la zone processed"

            echo "[INFO] Contenu de la zone processed :"
            docker exec namenode hdfs dfs -ls /data/ecommerce/logs/processed/
        """,
        trigger_rule="none_failed_min_one_success",
    )

    # ── Dépendances ──
    t_creer_repertoires >> t_generer_logs >> t_uploader >> t_verifier >> t_analyser >> t_decider
    t_decider >> [t_alerter, t_archive_ok]
    t_alerter >> t_archiver
    t_archive_ok >> t_archiver
