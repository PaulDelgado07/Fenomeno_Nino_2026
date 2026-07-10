import os
from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

# Configuración de conexión:
# Fuera de Docker, conectamos a localhost:5433. Dentro de Docker o si se define, usamos postgres:5432.
DATABASE_URL = os.getenv(
    "DATABASE_URL", 
    "postgresql://el_nino:el_nino_2026@localhost:5433/el_nino"
)

engine = create_engine(DATABASE_URL)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

Base = declarative_base()

def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()
