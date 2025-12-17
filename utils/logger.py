# Logger Kurulumu (utils/logger.py)
"""
async uyumluluk
"""
import os
import logging
from loguru import logger
from pathlib import Path
from config import config


class InterceptHandler(logging.Handler):
    """Logging handler for intercepting standard library logging messages"""
    
    def emit(self, record):
        # Get corresponding Loguru level if it exists
        try:
            level = logger.level(record.levelname).name
        except ValueError:
            level = record.levelno
        
        # Find caller from where originated the logged message
        frame, depth = logging.currentframe(), 2
        while frame and frame.f_code.co_filename == logging.__file__:
            frame = frame.f_back
            depth += 1
        
        logger.opt(depth=depth, exception=record.exc_info).log(
            level, record.getMessage()
        )


# utils/logger.py'de console log seviyesini değiştir: seviyesini DEBUG'a çıkar:
logger.add(
    lambda msg: print(msg, end=""),
    level="DEBUG",  # INFO yerine DEBUG
    format="<green>{time:YYYY-MM-DD HH:mm:ss}</green> | <level>{level: <8}</level> | <cyan>{name}</cyan>:<cyan>{function}</cyan>:<cyan>{line}</cyan> - <level>{message}</level>",
    colorize=True
)



def setup_logger():
    """Async uyumlu logger kurulumu"""
    
    # Mevcut loggers'ı temizle
    logger.remove()
    
    # Standard logging interception
    logging.basicConfig(handlers=[InterceptHandler()], level=logging.INFO, force=True)
    
    # Console logging - async uyumlu format
    logger.add(
        lambda msg: print(msg, end=""),  # Async ortamda güvenli console çıktısı
        level="INFO",
        format="<green>{time:YYYY-MM-DD HH:mm:ss}</green> | <level>{level: <8}</level> | <cyan>{name}</cyan>:<cyan>{function}</cyan>:<cyan>{line}</cyan> - <level>{message}</level>",
        colorize=True
    )
    
    # Main log file - config'den path ve retention kullan
    logger.add(
        config.paths.LOGS_DIR / "bot.log",
        rotation="5 MB",
        retention=f"{config.bot.LOG_RETENTION_DAYS} days",
        level="INFO",
        format="{time:YYYY-MM-DD HH:mm:ss} | {level: <8} | {name}:{function}:{line} - {message}",
        enqueue=True,  # Async uyumluluk için
        backtrace=True,
        diagnose=True
    )
    
    # Error log file
    logger.add(
        config.paths.LOGS_DIR / "errors.log",
        rotation="5 MB",
        retention=f"{config.bot.LOG_RETENTION_DAYS} days",
        level="ERROR",
        format="{time:YYYY-MM-DD HH:mm:ss} | {level: <8} | {name}:{function}:{line} - {message}",
        enqueue=True,  # Async uyumluluk için
        backtrace=True,
        diagnose=True
    )
    
    # Debug mode için ek logger (DEBUG seviyesi sadece debug modda)
    if os.getenv("DEBUG", "").lower() == "true":
        logger.add(
            config.paths.LOGS_DIR / "debug.log",
            rotation="5 MB",
            retention="7 days",
            level="DEBUG",
            format="{time:YYYY-MM-DD HH:mm:ss} | {level: <8} | {name}:{function}:{line} - {message}",
            enqueue=True,
            backtrace=True,
            diagnose=True
        )
    
    logger.info("✅ Logger başlatıldı - Async uyumlu")


# Global logger instance için shortcut
log = logger