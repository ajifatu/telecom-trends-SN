import os
import logging
import snowflake.connector
from dotenv import load_dotenv

# Configuration du logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Chargement des variables d'environnement
load_dotenv("/app/config/.env")

# Configuration Snowflake
SNOWFLAKE_ACCOUNT = os.environ.get('SNOWFLAKE_ACCOUNT')
SNOWFLAKE_USER = os.environ.get('SNOWFLAKE_USER')
SNOWFLAKE_PASSWORD = os.environ.get('SNOWFLAKE_PASSWORD')
SNOWFLAKE_DATABASE = os.environ.get('SNOWFLAKE_DATABASE')
SNOWFLAKE_SCHEMA = os.environ.get('SNOWFLAKE_SCHEMA')
SNOWFLAKE_WAREHOUSE = os.environ.get('SNOWFLAKE_WAREHOUSE')

def setup_snowflake():
    """Configure l'environnement Snowflake pour le projet"""
    try:
        # Connexion à Snowflake
        conn = snowflake.connector.connect(
            user=SNOWFLAKE_USER,
            password=SNOWFLAKE_PASSWORD,
            account=SNOWFLAKE_ACCOUNT
        )
        cursor = conn.cursor()
        
        logger.info("Connexion à Snowflake établie avec succès")
        
        # Création du warehouse si nécessaire
        cursor.execute(f"CREATE WAREHOUSE IF NOT EXISTS {SNOWFLAKE_WAREHOUSE} WITH WAREHOUSE_SIZE = 'XSMALL' AUTO_SUSPEND = 300 AUTO_RESUME = TRUE")
        logger.info(f"Warehouse {SNOWFLAKE_WAREHOUSE} créé ou vérifié")
        
        # Utilisation du warehouse
        cursor.execute(f"USE WAREHOUSE {SNOWFLAKE_WAREHOUSE}")
        
        # Création de la base de données si nécessaire
        cursor.execute(f"CREATE DATABASE IF NOT EXISTS {SNOWFLAKE_DATABASE}")
        logger.info(f"Base de données {SNOWFLAKE_DATABASE} créée ou vérifiée")
        
        # Utilisation de la base de données
        cursor.execute(f"USE DATABASE {SNOWFLAKE_DATABASE}")
        
        # Création du schéma si nécessaire
        cursor.execute(f"CREATE SCHEMA IF NOT EXISTS {SNOWFLAKE_SCHEMA}")
        logger.info(f"Schéma {SNOWFLAKE_SCHEMA} créé ou vérifié")
        
        # Utilisation du schéma
        cursor.execute(f"USE SCHEMA {SNOWFLAKE_DATABASE}.{SNOWFLAKE_SCHEMA}")
        
        # Création des tables
        # Table pour stocker les posts Facebook
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS FACEBOOK_POSTS (
            post_id VARCHAR(100) PRIMARY KEY,
            page_id VARCHAR(100),
            message TEXT,
            created_time TIMESTAMP_NTZ,
            reaction_count INTEGER,
            sentiment_score FLOAT,
            topic VARCHAR(50),
            scraped_at TIMESTAMP_NTZ
        )
        """)
        logger.info("Table FACEBOOK_POSTS créée ou vérifiée")
        
        # Table pour stocker les commentaires Facebook
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS FACEBOOK_COMMENTS (
            comment_id VARCHAR(100) PRIMARY KEY,
            post_id VARCHAR(100),
            message TEXT,
            created_time TIMESTAMP_NTZ,
            sentiment_score FLOAT,
            FOREIGN KEY (post_id) REFERENCES FACEBOOK_POSTS(post_id)
        )
        """)
        logger.info("Table FACEBOOK_COMMENTS créée ou vérifiée")
        
        # Vue pour l'analyse des sentiments par opérateur télécom
        cursor.execute("""
        CREATE OR REPLACE VIEW TELECOM_SENTIMENT_ANALYSIS AS
        SELECT 
            page_id AS operator,
            AVG(sentiment_score) AS avg_sentiment,
            COUNT(*) AS post_count,
            DATE_TRUNC('day', created_time) AS day
        FROM FACEBOOK_POSTS
        GROUP BY operator, day
        ORDER BY day DESC, avg_sentiment DESC
        """)
        logger.info("Vue TELECOM_SENTIMENT_ANALYSIS créée")
        
        # Vue pour l'analyse des sujets par opérateur
        cursor.execute("""
        CREATE OR REPLACE VIEW TELECOM_TOPIC_ANALYSIS AS
        SELECT 
            page_id AS operator,
            topic,
            COUNT(*) AS topic_count,
            AVG(sentiment_score) AS avg_sentiment
        FROM FACEBOOK_POSTS
        GROUP BY operator, topic
        ORDER BY operator, topic_count DESC
        """)
        logger.info("Vue TELECOM_TOPIC_ANALYSIS créée")
        
        # Fermeture des connexions
        cursor.close()
        conn.close()
        
        logger.info("Configuration Snowflake terminée avec succès")
        
    except Exception as e:
        logger.error(f"Erreur lors de la configuration de Snowflake: {e}")

if __name__ == "__main__":
    setup_snowflake()