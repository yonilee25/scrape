from sqlalchemy import create_engine, Column, String, Integer, DateTime, Float, Text
from sqlalchemy.orm import sessionmaker, declarative_base
from sqlalchemy.dialects.postgresql import JSONB
from datetime import datetime
from contextlib import contextmanager
import json
from libs.common.config import Settings

settings = Settings()

engine = None
SessionLocal = None
Base = declarative_base()

def get_engine():
    global engine, SessionLocal
    if engine is None:
        engine = create_engine(settings.DATABASE_URL, pool_pre_ping=True, future=True)
        SessionLocal = sessionmaker(bind=engine, autoflush=False, autocommit=False)
    return engine

@contextmanager
def get_session():
    global SessionLocal
    if SessionLocal is None:
        get_engine()
    session = SessionLocal()
    try:
        yield session
        session.commit()
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()

class Job(Base):
    __tablename__ = "jobs"
    id = Column(String, primary_key=True)
    person = Column(String, nullable=False)
    status = Column(String, default="queued")
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow)

class Source(Base):
    __tablename__ = "sources"
    id = Column(Integer, primary_key=True, autoincrement=True)
    job_id = Column(String, index=True, nullable=False)
    url = Column(Text, nullable=False)
    kind = Column(String, default="webpage")
    source = Column(String, default="unknown")
    title = Column(Text, nullable=True)
    published_at = Column(String, nullable=True)
    confidence = Column(Float, default=0.5)
    status = Column(String, default="new")

class Document(Base):
    __tablename__ = "documents"
    id = Column(Integer, primary_key=True, autoincrement=True)
    job_id = Column(String, index=True, nullable=False)
    source_id = Column(Integer, index=True, nullable=False)
    file_path = Column(Text, nullable=False)
    mime_type = Column(String, nullable=False)
    text_path = Column(Text, nullable=True)
    transcript_path = Column(Text, nullable=True)
    status = Column(String, default="fetched")

class Event(Base):
    __tablename__ = "events"
    id = Column(Integer, primary_key=True, autoincrement=True)
    job_id = Column(String, index=True, nullable=False)
    date = Column(String, nullable=True)
    event_text = Column(Text, nullable=False)
    # Store citations as a JSON string (portable across DBs)
    citations_json = Column(Text, nullable=True)

    @property
    def citations(self):
        try:
            return json.loads(self.citations_json) if self.citations_json else []
        except Exception:
            return []

    @citations.setter
    def citations(self, value):
        self.citations_json = json.dumps(value) if value else None
