"""
Main Bot Entry Point - Optimized with HandlerLoader
Async/Sync uyumlu, kod tekrarları temizlenmiş

main.py

kova - YENİ CONFIG YAPISIYLA GÜNCELLENDİ

"""

import asyncio
import os
import signal
import sys
from contextlib import asynccontextmanager
from aiogram import Bot, Dispatcher
from aiogram.enums import ParseMode
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.client.default import DefaultBotProperties
from aiohttp import web

from config import config

from utils.handler_loader import HandlerLoader
from utils.logger import setup_logger, logger

# Logger kurulumu
setup_logger()

# Port configuration - YENİ CONFIG YAPISI
HEALTH_CHECK_PORT = 8080
WEBHOOK_PORT = config.webhook.PORT  # config.webhook.PORT

class BotServer:
    """Bot server management with async/sync harmony"""
    
    def __init__(self):
        self.bot = None
        self.dp = None
        self.health_server = None
        self.webhook_runner = None
        self.shutdown_event = asyncio.Event()
        
    async def initialize_bot(self) -> None:
        """Initialize bot and dispatcher"""
        if not config.bot.TELEGRAM_TOKEN:  # config.bot.TELEGRAM_TOKEN
            raise ValueError("❌ HATA: Bot token bulunamadı!")
        
        storage = MemoryStorage()
        self.bot = Bot(
            token=config.bot.TELEGRAM_TOKEN,  # config.bot.TELEGRAM_TOKEN
            default=DefaultBotProperties(parse_mode=ParseMode.HTML),
        )
        self.dp = Dispatcher(storage=storage)

        # ✅ HandlerLoader ile otomatik router yükleme
        logger.info("🔄 Handler'lar yükleniyor...")
        loader = HandlerLoader(self.dp)
        load_result = await loader.load_handlers(self.dp)
        logger.info(f"✅ Handler yükleme tamamlandı: {load_result}")

    def setup_signal_handlers(self):
        """Setup signal handlers for graceful shutdown"""
        def signal_handler(signum, frame):
            logger.info(f"📡 Signal alındı: {signum}, graceful shutdown başlatılıyor...")
            self.shutdown_event.set()

        # SIGTERM ve SIGINT sinyallerini yakala
        signal.signal(signal.SIGTERM, signal_handler)
        signal.signal(signal.SIGINT, signal_handler)

    @asynccontextmanager
    async def health_check_server(self, port: int):
        """Async health check server context manager"""
        async def handle_health_check(reader, writer):
            """Async health check handler"""
            try:
                data = await reader.read(1024)
                if not data:
                    return

                request_line = data.decode().split('\r\n')[0]
                method, path, _ = request_line.split()
                
                if path == '/health':
                    response = (
                        "HTTP/1.1 200 OK\r\n"
                        "Content-Type: text/plain\r\n"
                        "Content-Length: 13\r\n\r\n"
                        "Bot is running"
                    )
                    writer.write(response.encode())
                    await writer.drain()  # ✅ Async drain eklendi
                else:
                    response = (
                        "HTTP/1.1 404 Not Found\r\n"
                        "Content-Type: text/plain\r\n\r\n"
                        "Not Found"
                    )
                    writer.write(response.encode())
                    await writer.drain()  # ✅ Async drain eklendi
                    
            except Exception as e:
                logger.error(f"Health check hatası: {e}")
                try:
                    response = (
                        "HTTP/1.1 500 Internal Server Error\r\n"
                        "Content-Type: text/plain\r\n\r\n"
                        "Error"
                    )
                    writer.write(response.encode())
                    await writer.drain()  # ✅ Async drain eklendi
                except Exception:
                    pass
            finally:
                writer.close()
                await writer.wait_closed()  # ✅ Async close

        server = await asyncio.start_server(
            handle_health_check, 
            "0.0.0.0", 
            port
        )
        logger.info(f"✅ Health check sunucusu {port} portunda başlatıldı")
        
        try:
            yield server
        finally:
            server.close()
            await server.wait_closed()
            logger.info("✅ Health check sunucusu kapatıldı")

    async def start_webhook_mode(self) -> None:
        """Start webhook mode with unified health check"""
        app = web.Application()
        app["dp"] = self.dp
        app["bot"] = self.bot

        # Webhook endpoint
        app.router.add_post("/webhook", self._webhook_handler)
        
        # Unified health endpoint
        app.router.add_get("/health", self._health_handler)

        self.webhook_runner = web.AppRunner(app)
        await self.webhook_runner.setup()
        
        site = web.TCPSite(self.webhook_runner, "0.0.0.0", WEBHOOK_PORT)
        await site.start()
        
        logger.info(f"🌐 Webhook sunucusu {WEBHOOK_PORT} portunda dinleniyor")

        # Set webhook - YENİ CONFIG YAPISI
        await self.bot.set_webhook(
            url=f"{config.webhook.WEBHOOK_URL}/webhook",  # config.webhook.WEBHOOK_URL
            secret_token=config.webhook.WEBHOOK_SECRET or None,  # config.webhook.WEBHOOK_SECRET
            drop_pending_updates=True,
        )
        logger.info("✅ Webhook Telegram'a bildirildi")

    async def start_polling_mode(self) -> None:
        """Start polling mode"""
        logger.info("🤖 Polling modu başlatılıyor...")
        await self.bot.delete_webhook(drop_pending_updates=True)
        await self.dp.start_polling(self.bot)

    async def _webhook_handler(self, request: web.Request) -> web.Response:
        """Unified webhook handler"""
        # Secret token kontrolü - YENİ CONFIG YAPISI
        if config.webhook.WEBHOOK_SECRET:  # config.webhook.WEBHOOK_SECRET
            token = request.headers.get('X-Telegram-Bot-Api-Secret-Token')
            if token != config.webhook.WEBHOOK_SECRET:  # config.webhook.WEBHOOK_SECRET
                return web.Response(status=403, text="Forbidden")
        
        try:
            update = await request.json()
            await self.dp.feed_webhook_update(self.bot, update)
            return web.Response(text="ok")
        except Exception as e:
            logger.error(f"Webhook hata: {e}")
            return web.Response(status=500, text="error")

    async def _health_handler(self, request: web.Request) -> web.Response:
        """Unified health check handler"""
        return web.Response(text="Bot is running")

    async def shutdown(self) -> None:
        """Graceful shutdown"""
        logger.info("🔴 Bot durduruluyor...")
        
        if self.webhook_runner:
            await self.webhook_runner.cleanup()
            logger.info("✅ Webhook runner temizlendi")
        
        if self.bot:
            await self.bot.session.close()
            logger.info("✅ Bot session kapatıldı")
        
        logger.info("✅ Bot başarıyla durduruldu")

async def main():
    """Optimized main function"""
    server = BotServer()
    
    try:
        # Signal handler'ları kur
        server.setup_signal_handlers()
        
        # Bot'u başlat
        await server.initialize_bot()
        
        # Health check server'ı context manager ile başlat
        async with server.health_check_server(HEALTH_CHECK_PORT):
            if config.webhook.USE_WEBHOOK:  # config.webhook.USE_WEBHOOK
                # Webhook modu
                logger.info("🚀 Webhook modu başlatıldı...")
                await server.start_webhook_mode()
                
                # Shutdown event'ini bekle
                await server.shutdown_event.wait()
            else:
                # Polling modu - shutdown event ile birlikte
                logger.info("🚀 Polling modu başlatıldı...")
                polling_task = asyncio.create_task(server.start_polling_mode())
                
                # ✅ DÜZELTİLMİŞ KISIM - Tüm task'lar create_task ile sarmalanmalı
                shutdown_task = asyncio.create_task(server.shutdown_event.wait())
                
                done, pending = await asyncio.wait(
                    [shutdown_task, polling_task],
                    return_when=asyncio.FIRST_COMPLETED
                )

                # Eğer shutdown event tetiklendiyse polling'i iptal et
                if server.shutdown_event.is_set():
                    polling_task.cancel()
                    try:
                        await polling_task
                    except asyncio.CancelledError:
                        logger.info("📡 Polling task iptal edildi")
                
    except KeyboardInterrupt:
        logger.info("⚠️ Keyboard interrupt - Bot kapatılıyor...")
    except Exception as e:
        logger.error(f"❌ Ana hata: {e}", exc_info=True)
    finally:
        # Graceful shutdown
        await server.shutdown()

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("✅ Bot kapatıldı")
    except Exception as e:
        logger.error(f"❌ Kritik hata: {e}", exc_info=True)