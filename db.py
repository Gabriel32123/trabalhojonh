from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from models import Base

engine = create_engine("sqlite:///transacoes.db")
Session = sessionmaker(bind=engine)
Base.metadata.create_all(engine)
