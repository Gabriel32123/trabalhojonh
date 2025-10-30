import json
import time
import uuid
import random
from datetime import datetime
from kafka import KafkaProducer


MAX_FRAUD_SEQUENCE = 5
fraud_sequence = 0

def criar_transacao():
    global fraud_sequence

    lista_cidades = [
        "Curitiba", "Porto Alegre", "Brasília", "Recife", "Salvador",
        "Buenos Aires", "Nova York", "Tóquio", "Paris", "Londres"
    ]

    # Controle de sequência de possíveis fraudes
    if fraud_sequence >= MAX_FRAUD_SEQUENCE:
        valor = round(random.uniform(100, 5000), 2)
        fraud_sequence = 0
    else:
        valor = round(random.uniform(100, 20000), 2)
        if valor > 15000:
            fraud_sequence += 1
        else:
            fraud_sequence = 0

    transacao = {
        "transaction_id": str(uuid.uuid4()),
        "client_id": random.randint(1, 10),
        "amount": valor,
        "city": random.choice(lista_cidades),
        "timestamp": datetime.now().isoformat()
    }

    return transacao


def iniciar_producer():
    produtor = KafkaProducer(
        bootstrap_servers=["localhost:9092"],
        value_serializer=lambda valor: json.dumps(valor).encode("utf-8")
    )

    topico = "transacoes"
    print("🚀 Producer ativo! Enviando transações a cada 3 segundos...\n")

    try:
        while True:
            evento = criar_transacao()
            produtor.send(topico, value=evento)
            print(f" Transação enviada → {evento}")
            time.sleep(3)
    except KeyboardInterrupt:
        print("\n Finalizando producer com segurança...")
        produtor.close()


if __name__ == "__main__":
    iniciar_producer()

