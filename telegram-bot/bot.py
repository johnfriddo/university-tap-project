import os
import logging
import json
import threading

from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import (
    Application, MessageHandler, filters, ContextTypes, CallbackQueryHandler
)
from confluent_kafka import Producer, Consumer, KafkaError

logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)
logger = logging.getLogger(__name__)

TELEGRAM_TOKEN = os.getenv('TELEGRAM_TOKEN')
KAFKA_BROKER_URL = os.getenv('KAFKA_BROKER_URL', 'kafka:9092')
KAFKA_TOPIC_INGESTION = 'telegram_messages'
KAFKA_TOPIC_NOTIFICATIONS = 'telegram_notifications'
KAFKA_CONSUMER_GROUP = 'telegram_bot_notifier'

async def handle_message(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    user_message = update.message.text
    
    keyboard = [
        [
            InlineKeyboardButton("✅ Procedi", callback_data=f"confirm:{user_message}"),
            InlineKeyboardButton("❌ Annulla", callback_data="cancel:"),
        ]
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)

    await update.message.reply_text(
        "📆 Hai inviato il seguente messaggio:\n\n"
        f"_{user_message}_\n\n"
        "Vuoi trasformarlo in un evento?",
        reply_markup=reply_markup,
        parse_mode='Markdown'
    )

async def button_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    await query.answer()

    action, data = query.data.split(":", 1)

    if action == "confirm":
        chat_id = query.message.chat_id
        user_message_text = data
        
        logger.info(f"Conferma ricevuta da chat_id {chat_id} per il messaggio: '{user_message_text}'")

        message_to_kafka = {'chat_id': chat_id, 'text': user_message_text}
        encoded_message = json.dumps(message_to_kafka).encode('utf-8')

        try:
            producer = context.bot_data['kafka_producer']
            producer.produce(KAFKA_TOPIC_INGESTION, value=encoded_message)
            producer.flush()
            logger.info(f"Messaggio inviato al topic Kafka '{KAFKA_TOPIC_INGESTION}'")

            await query.edit_message_text(text="✅ Richiesta confermata!\n\n Elaborazione in corso...")
        except Exception as e:
            logger.error(f"Errore durante l'invio del messaggio a Kafka: {e}")
            await query.edit_message_text(text="⚠️ Oops! qualcosa è andato storto durante l'invio della tua richiesta.")

    elif action == "cancel":

        logger.info(f"Operazione annullata da chat_id {query.message.chat_id}")
        await query.edit_message_text(text="❌ Richiesta annullata. Nessun evento è stato creato.\n\n"
         "Puoi inviarmi un nuovo messaggio quando vuoi.")

def get_kafka_producer():
    
    try:
        producer_conf = {'bootstrap.servers': KAFKA_BROKER_URL}
        producer = Producer(producer_conf)
        logger.info("Producer Kafka connesso con successo!")
        return producer
    except Exception as e:
        logger.error(f"Impossibile connettersi a Kafka come producer: {e}")
        return None

def notification_consumer(application: Application):

    consumer_conf = {
        'bootstrap.servers': KAFKA_BROKER_URL,
        'group.id': KAFKA_CONSUMER_GROUP,
        'auto.offset.reset': 'earliest'
    }

    consumer = Consumer(consumer_conf)
    consumer.subscribe([KAFKA_TOPIC_NOTIFICATIONS])
    logger.info("Consumer Kafka in ascolto sul topic delle notifiche...")
    
    while True:
        msg = consumer.poll(1.0)
        if msg is None: continue
        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF: continue
            else:
                logger.error(f"Errore del consumer Kafka: {msg.error()}")
                break
        try:
            notification = json.loads(msg.value().decode('utf-8'))
            chat_id = notification.get('chat_id')
            text = notification.get('text')
            if chat_id and text:
                logger.info(f"Ricevuta notifica per chat_id {chat_id}: '{text}'")
                application.job_queue.run_once(
                    lambda ctx: ctx.bot.send_message(chat_id=chat_id, text=text), 0
                )
        except (json.JSONDecodeError, KeyError) as e:
            logger.error(f"Errore nel decodificare o processare la notifica: {e}")
    consumer.close()

def main() -> None:
    application = Application.builder().token(TELEGRAM_TOKEN).build()
    
    kafka_producer = get_kafka_producer()
    if not kafka_producer:
        return
    application.bot_data['kafka_producer'] = kafka_producer

    consumer_thread = threading.Thread(
        target=notification_consumer,
        args=(application,),
        daemon=True
    )
    consumer_thread.start()

    application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_message))
    application.add_handler(CallbackQueryHandler(button_handler))

    logger.info("Bot avviato e in ascolto...")
    application.run_polling()

if __name__ == '__main__':
    main()