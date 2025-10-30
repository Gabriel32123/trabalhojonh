from sqlalchemy.orm import sessionmaker
from sqlalchemy import create_engine
from models import Transacao

# Conecta ao banco
engine = create_engine("sqlite:///transacoes.db")
Session = sessionmaker(bind=engine)
session = Session()

# Pega todas as transações
transacoes = session.query(Transacao).all()

print(f"🔹 Total de transações: {len(transacoes)}\n")

for t in transacoes:
    print(f"ID: {t.id} | Transaction_ID: {t.transaction_id} | Cliente: {t.client_id} | "
          f"Valor: {t.amount} | Cidade: {t.city} | Fraude: {t.tipo_fraude} | Timestamp: {t.timestamp}")

session.close()
