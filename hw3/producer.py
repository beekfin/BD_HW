import asyncio
import json
import re
import os
from datetime import datetime
from collections import Counter

from telethon import TelegramClient, events
from telethon.tl.functions.channels import JoinChannelRequest
from confluent_kafka import Producer

TG_API_ID = 35258931
TG_API_HASH = '46f77bb930c5d65da3fc5af65c0f9ea5'
KAFKA_BOOTSTRAP = 'localhost:9092'
KAFKA_TOPIC = 'telegram_words_stream'

MONITORED_CHANNELS = [
    'koroche_112', 
    'koroche_novostnoy', 
    'toporlive', 
    'rozetkedplus',
    'whackdoor',
    'Cbpub',
    'ixbtcom_news',
    'exploitex',
    'tengentoppagigachadgurrenlagann'
]

tg_client = TelegramClient('session_producer', TG_API_ID, TG_API_HASH)

kafka_conf = {
    'bootstrap.servers': KAFKA_BOOTSTRAP,
    'client.id': 'telegram-producer-v1'
}
kafka_sender = Producer(kafka_conf)

stats_buffer = Counter()
RUSSIAN_VOWELS = set('аеёиоуыэюя')

STOP_LIST = set(['в', 'а', 'с', 'и', 'но', 'я', 'ты', 'вы', 'он', 'мы', 'не', 'бы', 'же', 'то', 'за', 'по', 'на'])
try:
    s_path = os.path.join(os.path.dirname(__file__), 'stop-words-russian.txt')
    if os.path.exists(s_path):
        with open(s_path, 'r', encoding='utf-8') as f:
            STOP_LIST.update(l.strip().lower() for l in f if l.strip())
except Exception:
    pass

def sanitize_word(raw_word):
    cleaned = re.sub(r'[^а-яА-ЯёЁ]', '', raw_word)
    
    if len(cleaned) < 2:
        return None
        
    temp = cleaned
    while temp and temp[-1].lower() in RUSSIAN_VOWELS.union('й'):
        temp = temp[:-1]
        
    return temp if len(temp) >= 2 else None

def process_text(text_content):
    if not text_content:
        return []
        
    found_tokens = []
    candidates = re.findall(r'\b[А-ЯЁ][а-яё]*\b', text_content)
    
    if candidates:
        candidates = candidates[1:]
        
    for token in candidates:
        if token.lower() in STOP_LIST:
            continue
            
        processed = sanitize_word(token)
        if processed:
            found_tokens.append(processed)
            
    return found_tokens

def kafka_delivery_callback(err, msg):
    """Callback для подтверждения доставки в Kafka"""
    if err:
        print(f"Kafka Error: {err}")

async def scheduled_sender():
    """
    Фоновая задача: раз в минуту отправляет статистику и очищает буфер.
    """
    while True:
        await asyncio.sleep(60)
        
        if not stats_buffer:
            continue
            
        timestamp_str = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        
        message_payload = {
            'timestamp': timestamp_str,
            'stats': dict(stats_buffer)
        }
        
        try:
            payload_bytes = json.dumps(message_payload, ensure_ascii=False).encode('utf-8')
            kafka_sender.produce(
                KAFKA_TOPIC, 
                value=payload_bytes, 
                callback=kafka_delivery_callback
            )
            kafka_sender.poll(1)
            kafka_sender.flush()
            
            print(f"[{timestamp_str}] >> Отправлено в Kafka: {len(stats_buffer)} уникальных слов.")
            stats_buffer.clear()
            
        except Exception as e:
            print(f"Failed to send data: {e}")

@tg_client.on(events.NewMessage)
async def incoming_handler(event):
    """Обработчик входящих сообщений"""
    if not event.chat:
        return

    chat_uname = getattr(event.chat, 'username', '') or ''
    is_monitored = False
    
    if chat_uname:
        if chat_uname.lower() in [Target.lower() for Target in MONITORED_CHANNELS]:
            is_monitored = True
            
    if is_monitored:
        msg_text = event.message.message or event.message.raw_text or event.text
        
        if msg_text:
            extracted_words = process_text(msg_text)
            if extracted_words:
                stats_buffer.update(extracted_words)


async def main_loop():
    print("--- Запуск Telegram Client ---")
    await tg_client.start()
    
    print(f"--- Подписка на каналы ({len(MONITORED_CHANNELS)} шт) ---")
    for ch_name in MONITORED_CHANNELS:
        try:
            await tg_client(JoinChannelRequest(ch_name))
            print(f"OK: Подписан на @{ch_name}")
        except Exception as e:
            print(f"WARN: Не удалось подписаться на {ch_name}: {e}")
            
    sender_task = asyncio.create_task(scheduled_sender())
    
    print("--- Слушаю эфир... ---")
    try:
        await tg_client.run_until_disconnected()
    finally:
        sender_task.cancel()
        tg_client.disconnect()
        print("--- Работа завершена ---")

if __name__ == '__main__':
    with tg_client:
        tg_client.loop.run_until_complete(main_loop())
