import os
import json
import logging

import zoneinfo
from datetime import datetime, timezone

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StructType, StructField, StringType, LongType

import google.generativeai as genai
from google.oauth2 import service_account
from googleapiclient.discovery import build

from confluent_kafka import Producer

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

GEMINI_API_KEY = os.getenv('GEMINI_API_KEY')
GOOGLE_APPLICATION_CREDENTIALS = os.getenv('GOOGLE_APPLICATION_CREDENTIALS')
GOOGLE_CALENDAR_ID = os.getenv('GOOGLE_CALENDAR_ID', 'primary')

KAFKA_BROKER_URL = 'kafka:9092'
KAFKA_TOPIC_INGESTION = 'telegram_messages'
KAFKA_TOPIC_NOTIFICATIONS = 'telegram_notifications'
KAFKA_TOPIC_INDEXING = 'events_for_indexing'

genai.configure(api_key=GEMINI_API_KEY)
gemini_model = genai.GenerativeModel('gemini-2.5-flash')

def parse_event_with_gemini(text_message: str) -> dict | None:
    current_date = datetime.now().strftime("%Y-%m-%d")

    prompt = f"""
    Dato il seguente testo dell'utente, estrai le informazioni per creare un evento del calendario.
    La data di oggi è {current_date}.
    Il tuo compito è rispondere SOLO con un oggetto JSON valido, seguendo RIGOROSAMENTE la struttura fornita. Non aggiungere commenti o testo esplicativo.

    Regole importanti:
    1. I campi 'summary', 'start' e 'end' sono obbligatori.
    2. Il formato per 'dateTime' DEVE essere "YYYY-MM-DDTHH:MM:SS".
    3. La durata predefinita di un evento è di 1 ora.
    4. Se l'orario di fine di un evento supera le 23:59, deve essere impostato all'inizio del giorno successivo. Ad esempio, un evento che inizia alle 23:00 deve finire alle 00:00 del giorno dopo (YYYY-MM-DDT00:00:00, con la data incrementata di uno).
    5. Classifica l'evento in UNA sola delle seguenti categorie: [Routine e benessere, Lavoro/Studio, Cura personale, Famiglia e relazioni, Organizzazione e commissioni, Tempo libero e svago, Note e obiettivi del giorno]. Inserisci questa informazione nel campo extendedProperties.private.category.

    Il JSON deve avere questa struttura:
    {{
    "summary": "Titolo dell'evento",
    "start": {{ "dateTime": "YYYY-MM-DDTHH:MM:SS", "timeZone": "Europe/Rome" }},
    "end": {{ "dateTime": "YYYY-MM-DDTHH:MM:SS", "timeZone": "Europe/Rome" }},
    "extendedProperties": {{
        "private": {{
        "category": "categoria_evento"
        }}
    }}
    }}

    Se non riesci a determinare un evento valido dal testo, rispondi con: {{"error": "informazioni insufficienti"}}

    Testo utente: "{text_message}"

    JSON:
    """
    try:
        logging.info(f"Invio richiesta a Gemini per il testo: '{text_message}'")
        response = gemini_model.generate_content(prompt)
        cleaned_response = response.text.strip().replace('```json', '').replace('```', '')
        logging.info(f"Risposta JSON da Gemini: {cleaned_response}")

        # caso gemini non ha capito
        event_data = json.loads(cleaned_response)
        if "error" in event_data:
            logging.warning("Gemini non è riuscito a estrarre un evento valido dal testo.")
            return None
            
        return event_data
        
    except Exception as e:
        logging.error(f"Errore durante la chiamata a Gemini o nel parsing del JSON: {e}")
        return None

def create_google_calendar_event(event_data: dict) -> str:
    try:
        creds = service_account.Credentials.from_service_account_file(
            GOOGLE_APPLICATION_CREDENTIALS,
            scopes=['https://www.googleapis.com/auth/calendar']
        )
        service = build('calendar', 'v3', credentials=creds)
        
        event = service.events().insert(
            calendarId=GOOGLE_CALENDAR_ID,
            body=event_data
        ).execute()
        
        summary = event.get('summary', 'Evento')
        start_time = event['start'].get('dateTime', event['start'].get('date'))
        logging.info(f"Evento '{summary}' creato con successo per il {start_time}")
        return f"🎉 Evento '{summary}' creato con successo!"
    except Exception as e:
        logging.error(f"Errore durante la creazione dell'evento su Google Calendar: {e}")
        return f"⚠️ Non sono riuscito a creare l'evento. Dettaglio errore: {e}"

def process_batch(batch_df, epoch_id):
    if batch_df.count() == 0:
        return

    logging.info(f"--- Inizio elaborazione batch {epoch_id} ---")
    messages = batch_df.collect()
    
    kafka_conf = {'bootstrap.servers': KAFKA_BROKER_URL}
    producer = Producer(kafka_conf)
    
    for row in messages:
        chat_id = row.chat_id
        text = row.text
        
        event_json = parse_event_with_gemini(text)
        
        if event_json:
            confirmation_message = create_google_calendar_event(event_json)
            
            if "creato con successo" in confirmation_message:
                try:

                    created_at_obj_utc = datetime.now(timezone.utc)
                    start_time_str = event_json.get('start', {}).get('dateTime')
                    timezone_str = event_json.get('start', {}).get('timeZone', 'UTC') 

                    start_time_obj_aware = None

                    if start_time_str:
                        start_time_obj_naive = datetime.fromisoformat(start_time_str)
                        try:
                            tz = zoneinfo.ZoneInfo(timezone_str)
                            start_time_obj_aware = start_time_obj_naive.replace(tzinfo=tz)
                        except zoneinfo.ZoneInfoNotFoundError:
                            start_time_obj_aware = start_time_obj_naive.replace(tzinfo=timezone.utc)
                    
                    if not start_time_obj_aware:
                        start_time_obj_aware = created_at_obj_utc

                    doc_to_index = {
                        'summary': event_json.get('summary'),
                        'category': event_json.get('extendedProperties', {}).get('private', {}).get('category'),
                        'start_time': start_time_str,
                        'end_time': event_json.get('end', {}).get('dateTime'),
                        'timezone': timezone_str,
                        'created_at': created_at_obj_utc.isoformat(),
                        'original_text': text,
                        'user_id': str(chat_id),
                        'start_hour_of_day': start_time_obj_aware.hour,
                        'start_day_of_week_num': start_time_obj_aware.isoweekday(),
                        'start_day_of_week_name': start_time_obj_aware.strftime('%A'),
                        'original_text_length': len(text),
                        'planning_lead_time_hours': round((start_time_obj_aware - created_at_obj_utc).total_seconds() / 3600, 2)
                    }

                    encoded_doc = json.dumps(doc_to_index).encode('utf-8')
                    producer.produce(KAFKA_TOPIC_INDEXING, value=encoded_doc)
                    logging.info(f"Evento '{doc_to_index['summary']}' inviato a Kafka per l'indicizzazione.")

                except Exception as e:
                    logging.error(f"Errore durante l'invio a Kafka per l'indicizzazione: {e}")
        else:
            confirmation_message = "⚠️ Non sono riuscito a capire la tua richiesta. Prova a essere più specifico."

        notification = {'chat_id': chat_id, 'text': confirmation_message}
        encoded_notification = json.dumps(notification).encode('utf-8')
        producer.produce(KAFKA_TOPIC_NOTIFICATIONS, value=encoded_notification)
        logging.info(f"Notifica inviata per chat_id {chat_id}: '{confirmation_message}'")
    
    producer.flush()
    logging.info(f"--- Fine elaborazione batch {epoch_id} ---")

if __name__ == "__main__":
    spark = SparkSession \
        .builder \
        .appName("TelegramCalendarBotProcessor") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")
    logging.info("Sessione Spark creata.")

    schema = StructType([
        StructField("chat_id", LongType(), True),
        StructField("text", StringType(), True)
    ])

    kafka_df = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BROKER_URL) \
        .option("subscribe", KAFKA_TOPIC_INGESTION) \
        .option("startingOffsets", "latest") \
        .option("failOnDataLoss", "false") \
        .load()

    processed_df = kafka_df \
        .select(from_json(col("value").cast("string"), schema).alias("data")) \
        .select("data.*")

    query = processed_df \
        .writeStream \
        .foreachBatch(process_batch) \
        .start()

    logging.info("Stream in ascolto. In attesa di messaggi da Kafka...")
    query.awaitTermination()