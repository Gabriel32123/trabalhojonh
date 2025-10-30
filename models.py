from sqlalchemy import Column, Integer, String, Float, DateTime
from sqlalchemy.orm import declarative_base

Base = declarative_base()

class Transacao(Base):
    __tablename__ = "transacoes"

    id = Column(Integer, primary_key=True, autoincrement=True)
    transaction_id = Column(String, unique=True)
    client_id = Column(Integer)
    amount = Column(Float)
    city = Column(String)
    timestamp = Column(DateTime)
    tipo_fraude = Column(String, nullable=True)
