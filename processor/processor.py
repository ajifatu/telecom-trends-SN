import os
import json
import logging
import time
from kafka import KafkaConsumer
import snowflake.connector
from textblob import TextBlob
import nltk
from dotenv import load_dotenv

# Configuration du logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Téléchargement des ressources NLTK nécessaires
try:
    nltk.download('punkt', quiet=True)
    nltk.download('stopwords', quiet=True)
except Exception as e:
    logger.error(f"Erreur lors du téléchargement des ressources NLTK: {e}")

# Chargement des variables d'environnement
load_dotenv("/app/config/.env")

# Configuration Kafka
KAFKA_BOOTSTRAP_SERVERS = os.environ.get('KAFKA_BOOTSTRAP_SERVERS', 'kafka:29092')
KAFKA_TOPIC = 'facebook-posts'
KAFKA_GROUP_ID = 'facebook-processor'

# Configuration Snowflake
SNOWFLAKE_ACCOUNT = os.environ.get('SNOWFLAKE_ACCOUNT')
SNOWFLAKE_USER = os.environ.get('SNOWFLAKE_USER')
SNOWFLAKE_PASSWORD = os.environ.get('SNOWFLAKE_PASSWORD')
SNOWFLAKE_DATABASE = os.environ.get('SNOWFLAKE_DATABASE')
SNOWFLAKE_SCHEMA = os.environ.get('SNOWFLAKE_SCHEMA')
SNOWFLAKE_WAREHOUSE = os.environ.get('SNOWFLAKE_WAREHOUSE')

def connect_to_kafka():
    """Établit une connexion avec le consumer Kafka"""
    try:
        consumer = KafkaConsumer(
            KAFKA_TOPIC,
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            group_id=KAFKA_GROUP_ID,
            auto_offset_reset='earliest',
            value_deserializer=lambda x: json.loads(x.decode('utf-8'))
        )
        logger.info("Connexion au consumer Kafka établie avec succès")
        return consumer
    except Exception as e:
        logger.error(f"Erreur lors de la connexion au consumer Kafka: {e}")
        return None

def connect_to_snowflake():
    """Établit une connexion avec Snowflake"""
    try:
        conn = snowflake.connector.connect(
            user=SNOWFLAKE_USER,
            password=SNOWFLAKE_PASSWORD,
            account=SNOWFLAKE_ACCOUNT,
            warehouse=SNOWFLAKE_WAREHOUSE,
            database=SNOWFLAKE_DATABASE,
            schema=SNOWFLAKE_SCHEMA
        )
        logger.info("Connexion à Snowflake établie avec succès")
        return conn
    except Exception as e:
        logger.error(f"Erreur lors de la connexion à Snowflake: {e}")
        return None

def initialize_snowflake_tables(conn):
    """Initialise les tables Snowflake nécessaires"""
    try:
        cursor = conn.cursor()
        
        # Création de la table des posts
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS FACEBOOK_POSTS (
            post_id VARCHAR(100) PRIMARY KEY,
            page_id VARCHAR(100),
            message TEXT,
            created_time TIMESTAMP,
            reaction_count INTEGER,
            sentiment_score FLOAT,
            topic VARCHAR(50),
            scraped_at TIMESTAMP
        )
        """)
        
        # Création de la table des commentaires
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS FACEBOOK_COMMENTS (
            comment_id VARCHAR(100) PRIMARY KEY,
            post_id VARCHAR(100),
            message TEXT,
            created_time TIMESTAMP,
            sentiment_score FLOAT
        )
        """)
        
        logger.info("Tables Snowflake initialisées avec succès")
        cursor.close()
    except Exception as e:
        logger.error(f"Erreur lors de l'initialisation des tables Snowflake: {e}")

def analyze_sentiment(text):
    """Analyse le sentiment du texte"""
    try:
        if not text:
            return 0.0
        
        # Utilisation de TextBlob pour l'analyse de sentiment
        blob = TextBlob(text)
        # Le score est entre -1 (très négatif) et 1 (très positif)
        return blob.sentiment.polarity
    except Exception as e:
        logger.error(f"Erreur lors de l'analyse de sentiment: {e}")
        return 0.0

def categorize_topic(text):
    """Catégorise le sujet du texte"""
    if not text:
        return "Non catégorisé"
    
    # Mots-clés pour les catégories télécom courantes
    keywords = {
        "Réseau": ["réseau", "couverture", "signal", "4G", "5G", "internet", "connexion", "débit"],
        "Forfaits": ["forfait", "offre", "pack", "tarif", "prix", "abonnement", "recharge"],
        "Support": ["aide", "support", "assistance", "service client", "réclamation", "problème"],
        "Promotion": ["promo", "promotion", "offre spéciale", "réduction", "cadeau", "gratuit"],
        "Facturation": ["facture", "paiement", "débit", "crédit", "solde", "recharge"]
    }
    
    text_lower = text.lower()
    
    # Comptage des occurrences de mots-clés par catégorie
    category_scores = {category: 0 for category in keywords}
    
    for category, words in keywords.items():
        for word in words:
            if word.lower() in text_lower:
                category_scores[category] += 1
    
    # Sélection de la catégorie avec le plus d'occurrences
    max_category = max(category_scores.items(), key=lambda x: x[1])
    
    # Si aucun mot-clé n'a été trouvé, retourner "Autre"
    if max_category[1] == 0:
        return "Autre"
    
    return max_category[0]

def save_to_snowflake(conn, post):
    """Sauvegarde les données du post dans Snowflake"""
    try:
        cursor = conn.cursor()
        
        # Analyse du sentiment et catégorisation du sujet pour le post
        sentiment_score = analyze_sentiment(post.get('message', ''))
        topic = categorize_topic(post.get('message', ''))
        
        # Insertion ou mise à jour du post
        cursor.execute("""
        INSERT INTO FACEBOOK_POSTS (post_id, page_id, message, created_time, reaction_count, sentiment_score, topic, scraped_at)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE 
            message = VALUES(message),
            reaction_count = VALUES(reaction_count),
            sentiment_score = VALUES(sentiment_score),
            topic = VALUES(topic),
            scraped_at = VALUES(scraped_at)
        """, (
            post.get('id'),
            post.get('page_id'),
            post.get('message', ''),
            post.get('created_time'),
            post.get('reaction_count', 0),
            sentiment_score,
            topic,
            post.get('scraped_at')
        ))
        
        # Traitement des commentaires
        for comment in post.get('comments', []):
            comment_sentiment = analyze_sentiment(comment.get('message', ''))
            
            cursor.execute("""
            INSERT INTO FACEBOOK_COMMENTS (comment_id, post_id, message, created_time, sentiment_score)
            VALUES (%s, %s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE 
                message = VALUES(message),
                sentiment_score = VALUES(sentiment_score)
            """, (
                comment.get('id'),
                post.get('id'),
                comment.get('message', ''),
                comment.get('created_time'),
                comment_sentiment
            ))
        
        conn.commit()
        logger.info(f"Post {post.get('id')} sauvegardé dans Snowflake avec succès")
        
        cursor.close()
    except Exception as e:
        logger.error(f"Erreur lors de la sauvegarde des données dans Snowflake: {e}")

def process_data(consumer, conn):
    """Traite les données reçues de Kafka et les sauvegarde dans Snowflake"""
    try:
        # Initialiser les tables Snowflake si nécessaire
        initialize_snowflake_tables(conn)
        
        logger.info("Démarrage du traitement des messages Kafka...")
        
        # Traitement continu des messages
        for message in consumer:
            try:
                post = message.value
                logger.info(f"Traitement du post {post.get('id')} de la page {post.get('page_id')}")
                
                # Analyse et sauvegarde des données
                save_to_snowflake(conn, post)
                
            except Exception as e:
                logger.error(f"Erreur lors du traitement d'un message: {e}")
    
    except Exception as e:
        logger.error(f"Erreur dans le processus de traitement: {e}")

if __name__ == "__main__":
    # Initialiser les connexions
    retry_count = 0
    max_retries = 10
    
    while retry_count < max_retries:
        consumer = connect_to_kafka()
        conn = connect_to_snowflake()
        
        if consumer and conn:
            logger.info("Démarrage du processeur de données...")
            process_data(consumer, conn)
            break
        else:
            retry_count += 1
            wait_time = 10 * retry_count  # Temps d'attente progressif
            logger.warning(f"Tentative de connexion échouée. Nouvelle tentative dans {wait_time} secondes...")
            time.sleep(wait_time)
    
    if retry_count >= max_retries:
        logger.critical("Impossible de démarrer le processeur après plusieurs tentatives. Arrêt du programme.")