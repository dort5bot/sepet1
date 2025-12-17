# config.py - Geliştirilmiş Versiyon
# -------------------------------
#       KOVA
# -------------------------------
import os
import logging
from dataclasses import dataclass, field
from pathlib import Path
from typing import List, Optional
from dotenv import load_dotenv

# Environment yükleme
load_dotenv()

def parse_admin_ids(admin_ids_str: Optional[str]) -> List[int]:
    """Admin ID'lerini string'den integer listesine çevir"""
    if not admin_ids_str:
        return []
    
    try:
        # Çeşitli formatları destekle: "775252999,8063867757" veya "775252999"
        ids = [int(id_str.strip()) for id_str in admin_ids_str.split(',') if id_str.strip()]
        logging.info(f"✅ Parsed Admin IDs: {ids}")
        return ids
    except ValueError as e:
        logging.error(f"❌ Admin ID parsing error: {e}")
        return []

def parse_smtp_ports(smtp_server: str) -> List[int]:
    """SMTP sunucusuna göre portları belirle"""
    if "yandex" in smtp_server.lower():
        return [465]
    else:
        return [465, 587]

@dataclass
class DatabaseConfig:
    REDIS_URL: str = os.getenv("REDIS_URL", "redis://localhost:6379/0")

@dataclass
class EmailConfig:
    SMTP_SERVER: str = os.getenv("SMTP_SERVER", "smtp.gmail.com")
    SMTP_USERNAME: str = os.getenv("SMTP_USERNAME", "")
    SMTP_PASSWORD: str = os.getenv("SMTP_PASSWORD", "")
    SMTP_PORTS: List[int] = field(default_factory=list)
    PERSONAL_EMAIL: str = os.getenv("PERSONAL_EMAIL", "dersdep@gmail.com")
    INPUT_EMAIL: str = os.getenv("INPUT_EMAIL", "")
    
    def __post_init__(self):

        # 1) Eğer ENV'den string geliyorsa parse et
        if isinstance(self.SMTP_PORTS, str):
            self.SMTP_PORTS = [
                int(x.strip()) for x in self.SMTP_PORTS.split(",") if x.strip().isdigit()
            ]

        # 2) Hâlâ boşsa fallback kullan
        if not self.SMTP_PORTS:
            self.SMTP_PORTS = [465, 587]





 

@dataclass
class WebhookConfig:
    USE_WEBHOOK: bool = field(default_factory=lambda: os.getenv("USE_WEBHOOK", "False").lower() == "true")
    WEBHOOK_URL: str = os.getenv("WEBHOOK_URL", "")
    WEBHOOK_SECRET: str = os.getenv("WEBHOOK_SECRET", "")
    PORT: int = int(os.getenv("PORT", 10000))

@dataclass
class PathConfig:
    DATA_DIR: Path = field(default_factory=lambda: Path(__file__).parent / "data")
    INPUT_DIR: Path = field(init=False)
    OUTPUT_DIR: Path = field(init=False)
    GROUPS_DIR: Path = field(init=False)
    LOGS_DIR: Path = field(init=False)
    
    def __post_init__(self):
        self.INPUT_DIR = self.DATA_DIR / "input"
        self.OUTPUT_DIR = self.DATA_DIR / "output" 
        self.GROUPS_DIR = self.DATA_DIR / "groups"
        self.LOGS_DIR = self.DATA_DIR / "logs"
        self.TEMP_DIR = self.DATA_DIR / "temp"
        
        # for directory in [self.INPUT_DIR, self.OUTPUT_DIR, self.GROUPS_DIR, self.LOGS_DIR]:
            # directory.mkdir(parents=True, exist_ok=True)

        for directory in [self.INPUT_DIR,
            self.OUTPUT_DIR,self.GROUPS_DIR,
            self.LOGS_DIR,self.TEMP_DIR,
        ]:
            directory.mkdir(parents=True, exist_ok=True)


@dataclass
class BotConfig:
    TELEGRAM_TOKEN: str = os.getenv("TELEGRAM_TOKEN", "")
    ADMIN_IDS: List[int] = field(default_factory=lambda: parse_admin_ids(os.getenv("ADMIN_IDS")))
    
    # Constants
    MAX_EMAIL_RETRIES: int = 2
    CHUNK_SIZE: int = 5000
    LOG_RETENTION_DAYS: int = 30
    # BÜYÜK DOSYA DESTEĞİ İÇİN
    MAX_MEMORY_USAGE_MB: int = 500  # Yeni: Memory limit
    MAX_FILE_SIZE_MB: int = 200  # Yeni: Max dosya boyutu
    BATCH_PROCESSING_SIZE: int = 1000  # Yeni: Batch boyutu


@dataclass
class Config:
    bot: BotConfig = field(default_factory=BotConfig)
    email: EmailConfig = field(default_factory=EmailConfig)
    webhook: WebhookConfig = field(default_factory=WebhookConfig)
    database: DatabaseConfig = field(default_factory=DatabaseConfig)
    paths: PathConfig = field(default_factory=PathConfig)

# Global config instance
config = Config()

# Debug info
logging.info(f"Admin IDs loaded: {config.bot.ADMIN_IDS}")
logging.info(f"SMTP Ports: {config.email.SMTP_PORTS}")
