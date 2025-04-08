import os
import json
import time
import logging
from datetime import datetime, timedelta
import facebook
from kafka import KafkaProducer
from dotenv import load_dotenv

# Configuration du logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Chargement des variables d'environnement
load_dotenv("/app/config/.env")

# Configuration Kafka
KAFKA_BOOTSTRAP_SERVERS = os.environ.get('KAFKA_BOOTSTRAP_SERVERS', 'kafka:29092')
KAFKA_TOPIC = 'facebook-posts'

# Configuration Facebook
FACEBOOK_ACCESS_TOKEN = os.environ.get('FACEBOOK_ACCESS_TOKEN')
# Liste des pages des opérateurs télécoms au Sénégal à suivre
TELECOM_PAGES = [
    'OrangeSenegal',
    'FreeSenegal', 
    'ExpressoSenegal'
]

def connect_to_kafka():
    """Établit une connexion avec le broker Kafka"""
    try:
        producer = KafkaProducer(
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )
        logger.info("Connexion à Kafka établie avec succès")
        return producer
    except Exception as e:
        logger.error(f"Erreur lors de la connexion à Kafka: {e}")
        return None

def connect_to_facebook():
    """Établit une connexion avec l'API Facebook"""
    try:
        graph = facebook.GraphAPI(access_token=FACEBOOK_ACCESS_TOKEN, version="3.1")
        logger.info("Connexion à l'API Facebook établie avec succès")
        return graph
    except Exception as e:
        logger.error(f"Erreur lors de la connexion à l'API Facebook: {e}")
        return None

def get_page_posts(graph, page_id, limit=10):
    """Récupère les posts récents d'une page Facebook"""
    try:
        # Récupérer les posts avec les commentaires et réactions
        posts = graph.get_connections(
            page_id, 
            'posts', 
            fields='id,message,created_time,comments.limit(10){message,created_time},reactions.summary(true)'
        )
        
        # Traiter les résultats
        results = []
        if 'data' in posts:
            for post in posts['data'][:limit]:
                # Extraction des données pertinentes
                post_data = {
                    'id': post.get('id'),
                    'page_id': page_id,
                    'message': post.get('message', ''),
                    'created_time': post.get('created_time'),
                    'comments': [],
                    'reaction_count': post.get('reactions', {}).get('summary', {}).get('total_count', 0),
                    'scraped_at': datetime.now().isoformat()
                }
                
                # Extraction des commentaires
                if 'comments' in post and 'data' in post['comments']:
                    for comment in post['comments']['data']:
                        post_data['comments'].append({
                            'id': comment.get('id'),
                            'message': comment.get('message', ''),
                            'created_time': comment.get('created_time')
                        })
                
                results.append(post_data)
                
        return results
    except Exception as e:
        logger.error(f"Erreur lors de la récupération des posts de {page_id}: {e}")
        return []

def run_scraper(producer, graph):
    """Exécute le processus de scraping et envoie les données à Kafka"""
    while True:
        try:
            for page in TELECOM_PAGES:
                logger.info(f"Récupération des posts de la page: {page}")
                posts = get_page_posts(graph, page)
                
                # Envoi des posts à Kafka
                for post in posts:
                    producer.send(KAFKA_TOPIC, value=post)
                    logger.info(f"Post {post['id']} envoyé à Kafka")
                
                logger.info(f"{len(posts)} posts récupérés et envoyés pour {page}")
            
            # Attendre 15 minutes avant la prochaine exécution
            logger.info("Attente de 15 minutes avant la prochaine récupération...")
            time.sleep(900)  # 15 minutes
            
        except Exception as e:
            logger.error(f"Erreur dans le processus de scraping: {e}")
            # En cas d'erreur, attendre 5 minutes avant de réessayer
            time.sleep(300)  # 5 minutes

if __name__ == "__main__":
    # Initialiser les connexions
    producer = connect_to_kafka()
    graph = connect_to_facebook()
    
    if producer and graph:
        logger.info("Démarrage du scraper Facebook...")
        run_scraper(producer, graph)
    else:
        logger.error("Impossible de démarrer le scraper en raison d'erreurs de connexion.")