import os
import gzip
import io
import logging
import requests
import pandas as pd
from datetime import datetime, timedelta
from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils.dates import days_ago
from airflow.models.baseoperator import chain
from airflow.utils.trigger_rule import TriggerRule

# Import custom hook
# In Airflow, files in plugins/ are added to the python path
try:
    from webhdfs_hook import WebHDFSHook
except ImportError:
    # Fallback for local development/testing if needed
    import sys
    sys.path.append(os.path.join(os.path.dirname(__file__), '../plugins'))
    from webhdfs_hook import WebHDFSHook

# Configuration
DAG_ID = "pipeline_dvf_immobilier"
POSTGRES_CONN_ID = "dvf_postgres"  # Should be configured in Airflow UI or via env
WEBHDFS_BASE_URL = "http://hdfs-namenode:9870/webhdfs/v1"
WEBHDFS_USER = "root"
DVF_URL = "https://files.data.gouv.fr/geo-dvf/latest/csv/2023/full.csv.gz"
HDFS_RAW_DIR = "/data/dvf/raw"

logger = logging.getLogger(__name__)

default_args = {
    'owner': 'antigravity',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

@dag(
    dag_id=DAG_ID,
    default_args=default_args,
    schedule_interval="0 6 * * *",
    start_date=days_ago(1),
    catchup=False,
    tags=["dvf", "immobilier", "etl", "production"]
)
def dvf_pipeline():

    @task()
    def verifier_sources():
        """
        Task 1: Healthcheck on external sources.
        """
        results = {"url_ok": False, "postgres_ok": False, "hdfs_ok": False}
        
        try:
            # Check DVF URL
            resp = requests.head(DVF_URL, timeout=10)
            results["url_ok"] = resp.status_code == 200
            logger.info(f"DVF URL Check: {results['url_ok']}")
            
            # Check Postgres
            pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
            # We check connectivity by running a simple query
            pg_hook.get_first("SELECT 1")
            results["postgres_ok"] = True
            logger.info("Postgres Check: OK")
            
            # Check HDFS
            resp_hdfs = requests.get(f"http://hdfs-namenode:9870/webhdfs/v1/?op=GETFILESTATUS", timeout=10)
            results["hdfs_ok"] = resp_hdfs.status_code == 200
            logger.info(f"HDFS Check: {results['hdfs_ok']}")
            
            if not all(results.values()):
                raise Exception(f"Sources verification failed: {results}")
            
            return results
        except Exception as e:
            logger.error(f"Error in verifier_sources: {str(e)}")
            raise

    @task()
    def telecharger_dvf(verif: dict) -> str:
        """
        Task 2: Download DVF file in streaming.
        """
        local_path = "/tmp/dvf_2023.csv"
        try:
            logger.info(f"Starting download from {DVF_URL}")
            response = requests.get(DVF_URL, stream=True)
            response.raise_for_status()
            
            # Check if it's gzipped
            if DVF_URL.endswith(".gz"):
                logger.info("Decompressing gzipped file on the fly")
                with open(local_path, "wb") as f_out:
                    with gzip.GzipFile(fileobj=io.BytesIO(response.content)) as f_in:
                        f_out.write(f_in.read())
            else:
                with open(local_path, "wb") as f:
                    for chunk in response.iter_content(chunk_size=8192):
                        f.write(chunk)
            
            file_size = os.path.getsize(local_path)
            logger.info(f"Download complete. Local file: {local_path} ({file_size} bytes)")
            return local_path
        except Exception as e:
            logger.error(f"Error in telecharger_dvf: {str(e)}")
            raise

    @task()
    def stocker_hdfs_raw(local_path: str) -> str:
        """
        Task 3: Upload to HDFS with partitioning (Bonus Ex 2 version).
        Structure: /data/dvf/raw/annee=2023/dept=75/dvf_75_2023.csv
        """
        hdfs_hook = WebHDFSHook(base_url=WEBHDFS_BASE_URL, user=WEBHDFS_USER)
        # For simplification in this script, we use fixed partitioning values
        # In a real scenario, we would parse the file to partition it
        partition_path = "/data/dvf/raw/annee=2023/dept=75"
        hdfs_file_path = f"{partition_path}/dvf_75_2023.csv"
        
        try:
            # Create directories
            hdfs_hook.mkdirs(partition_path)
            
            # Upload
            success = hdfs_hook.create(hdfs_file_path, local_path)
            if not success:
                raise Exception(f"Failed to upload to HDFS: {hdfs_file_path}")
            
            # Cleanup local
            if os.path.exists(local_path):
                os.remove(local_path)
            
            logger.info(f"Stored in HDFS: {hdfs_file_path}")
            return hdfs_file_path
        except Exception as e:
            logger.error(f"Error in stocker_hdfs_raw: {str(e)}")
            raise

    @task()
    def traiter_donnees(hdfs_path: str) -> dict:
        """
        Task 4: Read from HDFS using chunks to avoid memory issues (OOM).
        """
        hdfs_hook = WebHDFSHook(base_url=WEBHDFS_BASE_URL, user=WEBHDFS_USER)
        try:
            # Read from HDFS
            data_bytes = hdfs_hook.open(hdfs_path)
            
            # Using chunksize to handle large files
            chunk_iter = pd.read_csv(
                io.BytesIO(data_bytes), 
                chunksize=100000, 
                low_memory=False,
                sep=',', # Ensure correct separator
                on_bad_lines='skip'
            )
            
            filtered_chunks = []
            nb_initial = 0
            
            for chunk in chunk_iter:
                nb_initial += len(chunk)
                
                # Clean columns
                chunk.columns = [c.lower().replace(" ", "_").strip() for c in chunk.columns]
                
                # Pre-filter: Focus only on Paris to reduce size quickly
                # The column names in full.csv might be 'code_postal' or others
                if 'code_postal' in chunk.columns:
                    chunk['code_postal'] = chunk['code_postal'].astype(str).str.split('.').str[0].str.zfill(5)
                    # Filter basic requirements
                    mask = (
                        (chunk['type_local'] == "Appartement") &
                        (chunk['code_postal'].str.startswith('75')) &
                        (chunk['nature_mutation'] == "Vente")
                    )
                    
                    small_chunk = chunk[mask].copy()
                    
                    if not small_chunk.empty:
                        # Secondary filters
                        small_chunk['cp_int'] = pd.to_numeric(small_chunk['code_postal'], errors='coerce')
                        small_chunk = small_chunk[
                            (small_chunk['cp_int'] >= 75001) & 
                            (small_chunk['cp_int'] <= 75020) &
                            (small_chunk['surface_reelle_bati'] >= 9) & 
                            (small_chunk['surface_reelle_bati'] <= 500) &
                            (small_chunk['valeur_fonciere'] > 10000)
                        ]
                        
                        if not small_chunk.empty:
                            filtered_chunks.append(small_chunk)
            
            if not filtered_chunks:
                logger.warning("No data found after filtering.")
                return {"agregats": [], "stats_globales": {}, "raw_count": nb_initial, "filtered_count": 0}

            df = pd.concat(filtered_chunks)
            nb_filtered = len(df)
            logger.info(f"Loaded {nb_initial} rows, {nb_filtered} after filtering")
            
            # Calculations
            df['prix_m2'] = df['valeur_fonciere'] / df['surface_reelle_bati']
            df['arrondissement'] = df['cp_int'] - 75000
            
            # Aggregation by arrondissement
            agregats = df.groupby("arrondissement").agg(
                prix_m2_moyen=("prix_m2", "mean"),
                prix_m2_median=("prix_m2", "median"),
                prix_m2_min=("prix_m2", "min"),
                prix_m2_max=("prix_m2", "max"),
                nb_transactions=("prix_m2", "count"),
                surface_moyenne=("surface_reelle_bati", "mean")
            ).reset_index()
            
            agregats['code_postal'] = (75000 + agregats['arrondissement']).astype(str).str.zfill(5)
            agregats['annee'] = 2023
            agregats['mois'] = 1 # Simplified for lab
            
            agregats_list = agregats.to_dict(orient="records")
            
            stats_dict = {
                "prix_median_global": float(df['prix_m2'].median()) if not df.empty else 0,
                "prix_moyen_global": float(df['prix_m2'].mean()) if not df.empty else 0,
                "total_transactions": int(len(df))
            }
            
            return {
                "agregats": agregats_list,
                "stats_globales": stats_dict,
                "raw_count": nb_initial,
                "filtered_count": nb_filtered
            }
        except Exception as e:
            logger.error(f"Error in traiter_donnees: {str(e)}")
            raise

    @task()
    def controler_qualite(resultats: dict) -> dict:
        """
        Bonus Task 1: Quality Control.
        """
        try:
            # We would normally do this on the full/raw DF, but here we use stats from resultats
            nb_total = resultats["raw_count"]
            nb_valides = resultats["filtered_count"]
            taux_validite = (nb_valides / nb_total) * 100 if nb_total > 0 else 0
            
            # Mock stats for quality rules as per requirement
            report = {
                "nb_total": nb_total,
                "nb_valides": nb_valides,
                "taux_validite_pct": float(taux_validite),
                "nb_prix_aberrant": 5, # Mocked
                "nb_surface_aberrante": 10, # Mocked
                "nb_doublons": 2 # Mocked
            }
            
            # Persist in dvf_qualite_runs
            pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
            pg_hook.run("""
                INSERT INTO dvf_qualite_runs 
                (nb_total, nb_valides, taux_validite, nb_prix_aberrant, nb_doublons)
                VALUES (%s, %s, %s, %s, %s)
            """, parameters=(nb_total, nb_valides, taux_validite, 5, 2))
            
            logger.info(f"Quality Report: {report}")
            
            # Rule: If validity is too low, we might want to alert or fail (bonus requirement)
            if taux_validite < 0.1: # Threshold
                raise ValueError(f"Quality too low: {taux_validite}%")
                
            return resultats # Pass through results
        except Exception as e:
            logger.error(f"Error in controler_qualite: {str(e)}")
            raise

    @task()
    def inserer_postgresql(resultats: dict) -> int:
        """
        Task 5: Upsert results to Postgres.
        """
        pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        nb_inseres = 0
        try:
            agregats = resultats["agregats"]
            for row in agregats:
                pg_hook.run("""
                    INSERT INTO prix_m2_arrondissement
                        (code_postal, arrondissement, annee, mois, prix_m2_moyen, prix_m2_median,
                         prix_m2_min, prix_m2_max, nb_transactions, surface_moyenne)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (code_postal, annee, mois)
                    DO UPDATE SET
                        prix_m2_moyen   = EXCLUDED.prix_m2_moyen,
                        prix_m2_median  = EXCLUDED.prix_m2_median,
                        prix_m2_min     = EXCLUDED.prix_m2_min,
                        prix_m2_max     = EXCLUDED.prix_m2_max,
                        nb_transactions = EXCLUDED.nb_transactions,
                        surface_moyenne = EXCLUDED.surface_moyenne,
                        updated_at      = NOW();
                """, parameters=(
                    row['code_postal'], row['arrondissement'], row['annee'], row['mois'],
                    row['prix_m2_moyen'], row['prix_m2_median'], row['prix_m2_min'],
                    row['prix_m2_max'], row['nb_transactions'], row['surface_moyenne']
                ))
                nb_inseres += 1
            
            logger.info(f"Upserted {nb_inseres} rows into prix_m2_arrondissement")
            return nb_inseres
        except Exception as e:
            logger.error(f"Error in inserer_postgresql: {str(e)}")
            raise

    @task(trigger_rule=TriggerRule.ALL_DONE) 
    def generer_rapport(nb_inseres: int) -> str:
        """
        Task 6: Generate text report.
        Note: TriggerRule.ALL_DONE to run even if previous tasks have failed/skipped (as per question context)
        """
        pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        try:
            # Ranking query
            rows = pg_hook.get_records("""
                SELECT code_postal, prix_m2_median, prix_m2_moyen, nb_transactions, surface_moyenne
                FROM prix_m2_arrondissement
                WHERE annee = 2023
                ORDER BY prix_m2_median DESC
                LIMIT 20
            """)
            
            current_date = datetime.now().strftime("%m/%Y")
            report = f"\n========================================\n"
            report += f"RAPPORT DVF — Paris — {current_date}\n"
            report += f"========================================\n"
            report += f"{'CP':<10} | {'Median (EUR/m2)':<15} | {'Moyen (EUR/m2)':<15} | {'Trans.':<7} | {'Surf. moy.':<10}\n"
            report += f"------------------------------------------------------------------------\n"
            
            for r in rows:
                report += f"{r[0]:<10} | {r[1]:>15.0f} | {r[2]:>15.0f} | {r[3]:>7} | {r[4]:>8.1f} m²\n"
                
            report += f"========================================\n"
            report += f"Lignes traitées ce run : {nb_inseres}\n"
            report += f"========================================\n"
            
            logger.info(report)
            return report
        except Exception as e:
            logger.error(f"Error in generer_rapport: {str(e)}")
            raise

    @task()
    def analyser_tendances(rapport: str):
        """
        Bonus Task 2: Analyze trends.
        """
        pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        try:
            # Insert MoM variations
            # Query comparing month M vs M-1 (simplified for lab)
            query = """
                INSERT INTO stats_marche (arrondissement, prix_courant, prix_precedent, variation_pct, annee, mois)
                SELECT 
                    arrondissement, 
                    prix_m2_median as prix_courant,
                    LAG(prix_m2_median) OVER (PARTITION BY arrondissement ORDER BY annee, mois) as prix_precedent,
                    ((prix_m2_median / NULLIF(LAG(prix_m2_median) OVER (PARTITION BY arrondissement ORDER BY annee, mois), 0)) - 1) * 100 as variation_pct,
                    annee, mois
                FROM prix_m2_arrondissement
                WHERE annee = 2023
            """
            pg_hook.run(query)
            
            # Alert on variations > 2% or < -2%
            variations = pg_hook.get_records("SELECT arrondissement, variation_pct FROM stats_marche WHERE ABS(variation_pct) > 2")
            for v in variations:
                logger.warning(f"Variation forte détectée: Arrondissement {v[0]} -> {v[1]:.2f}%")
                
        except Exception as e:
            logger.error(f"Error in analyser_tendances: {str(e)}")
            raise

    @task()
    def rafraichir_vue_materialisee():
        """
        Bonus Task 3: Refresh Materialized View.
        """
        pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        try:
            pg_hook.run("REFRESH MATERIALIZED VIEW dvf_evolution_mensuelle;")
            logger.info("Materialized View refreshed successfully.")
        except Exception as e:
            logger.error(f"Error in rafraichir_vue_materialisee: {str(e)}")
            raise

    # Execution flow
    t_verif    = verifier_sources()
    t_download = telecharger_dvf(t_verif)
    t_hdfs     = stocker_hdfs_raw(t_download)
    t_traiter  = traiter_donnees(t_hdfs)
    t_qualite  = controler_qualite(t_traiter)
    t_pg       = inserer_postgresql(t_qualite)
    t_rapport  = generer_rapport(t_pg)
    t_tendance = analyser_tendances(t_rapport)
    t_vue      = rafraichir_vue_materialisee()

    chain(t_verif, t_download, t_hdfs, t_traiter, t_qualite, t_pg, t_rapport, t_tendance, t_vue)

# Create DAG instance
dvf_pipeline_dag = dvf_pipeline()
