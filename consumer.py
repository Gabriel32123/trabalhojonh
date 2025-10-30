from kafka import KafkaConsumer
import json
from datetime import datetime, timedelta
from collections import defaultdict
from db import Session
from models import Transacao
import logging


logging.getLogger("kafka").setLevel(logging.WARNING)
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

consumer = KafkaConsumer(
    'transacoes',
    bootstrap_servers=['localhost:9092'],
    value_deserializer=lambda v: json.loads(v.decode('utf-8')),
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    group_id='fraude-detector'
)

logging.info("✅ Consumer conectado ao Kafka — ouvindo o tópico 'transacoes'...")


historico = defaultdict(list)

def verifica_fraude(transacao):
    cliente = transacao["client_id"]
    valor = transacao["amount"]
    cidade = transacao["city"]
    timestamp = datetime.fromisoformat(transacao["timestamp"])

    
    if valor >= 10000:
        return "ALTO_VALOR"

    
    historico[cliente].append((timestamp, cidade))

   
    historico[cliente] = [
        (t, c) for (t, c) in historico[cliente]
        if timestamp - t <= timedelta(minutes=10)
    ]

    
    ultimas_60s = [t for (t, _) in historico[cliente] if timestamp - t <= timedelta(seconds=60)]
    if len(ultimas_60s) >= 4:
        return "TEMPO_60s"

    
    cidades_ultimas = {c for (t, c) in historico[cliente] if timestamp - t <= timedelta(minutes=10)}
    if len(cidades_ultimas) >= 2:
        return "GEO_10m"

    return None



session = Session()


try:
    for msg in consumer:
        transacao = msg.value
        fraude = verifica_fraude(transacao)

        nova_transacao = Transacao(
            transaction_id=transacao["transaction_id"],
            client_id=transacao["client_id"],
            amount=transacao["amount"],
            city=transacao["city"],
            timestamp=datetime.fromisoformat(transacao["timestamp"]),
            tipo_fraude=fraude
        )

        session.add(nova_transacao)
        session.commit()

        if fraude:
            logging.warning(f"🚨 FRAUDE DETECTADA: {fraude} | "
                            f"Cliente: {transacao['client_id']} | "
                            f"Valor: {transacao['amount']} | "
                            f"Cidade: {transacao['city']}")
        else:
            logging.info(f"✅ Transação normal: Cliente {transacao['client_id']} - "
                         f"{transacao['amount']} em {transacao['city']}")

except KeyboardInterrupt:
    logging.info("🛑 Encerrando consumer...")
    session.close()
