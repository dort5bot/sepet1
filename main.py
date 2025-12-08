"""
Main Bot Entry Point - Optimized with HandlerLoader
Async/Sync uyumlu, kod tekrarları temizlenmiş
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
from utils.mailer import get_default_mailer # Mailer.stop için

from utils.logger import setup_logger, logger
# Logger kurulumu
setup_logger()

class BotServer:
    """Bot server management with async/sync harmony"""
    
    def __init__(self):
        self.bot = None
        self.dp = None
        self.webhook_runner = None
        self.shutdown_event = asyncio.Event()
        
    async def initialize_bot(self) -> None:
        """Initialize bot and dispatcher"""
        if not config.bot.TELEGRAM_TOKEN:
            raise ValueError("❌ HATA: Bot token bulunamadı!")
        
        storage = MemoryStorage()
        self.bot = Bot(
            token=config.bot.TELEGRAM_TOKEN,
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

    async def start_webhook_mode(self) -> None:
        """Start webhook mode"""
        app = web.Application()
        app["dp"] = self.dp
        app["bot"] = self.bot

        # Webhook endpoint
        app.router.add_post("/webhook", self._webhook_handler)
        
        # Health endpoint
        app.router.add_get("/health", self._health_handler)

        self.webhook_runner = web.AppRunner(app)
        await self.webhook_runner.setup()
        
        site = web.TCPSite(self.webhook_runner, "0.0.0.0", config.webhook.PORT)
        await site.start()
        
        logger.info(f"🌐 Webhook sunucusu {config.webhook.PORT} portunda dinleniyor")

        # Set webhook
        await self.bot.set_webhook(
            url=f"{config.webhook.WEBHOOK_URL}/webhook",
            secret_token=config.webhook.WEBHOOK_SECRET or None,
            drop_pending_updates=True,
        )
        logger.info("✅ Webhook Telegram'a bildirildi")

    async def start_polling_mode(self) -> None:
        """Start polling mode"""
        logger.info("🤖 Polling modu başlatılıyor...")
        await self.bot.delete_webhook(drop_pending_updates=True)

        try:
            await self.dp.start_polling(self.bot)
        except asyncio.CancelledError:
            logger.info("📡 Polling durduruluyor...")
            await self.dp.stop_polling()
            raise

    async def _webhook_handler(self, request: web.Request) -> web.Response:
        """Webhook handler"""
        # Secret token kontrolü
        if config.webhook.WEBHOOK_SECRET:
            token = request.headers.get('X-Telegram-Bot-Api-Secret-Token')
            if token != config.webhook.WEBHOOK_SECRET:
                return web.Response(status=403, text="Forbidden")
        
        try:
            update = await request.json()
            await self.dp.feed_webhook_update(self.bot, update)
            return web.Response(text="ok")
        except Exception as e:
            logger.error(f"Webhook hata: {e}")
            return web.Response(status=500, text="error")

    async def _health_handler(self, request: web.Request) -> web.Response:
        """Health check handler"""
        return web.Response(text="Bot is running")


    async def shutdown(self) -> None:
        """Graceful shutdown"""
        logger.info("🔴 Bot durduruluyor...")

        # 1) SMTP bağlantısını temizle (kritik)
        try:
            from utils.mailer import get_default_mailer
            mailer = await get_default_mailer()
            await mailer.stop()
            logger.info("📨 Mailer SMTP bağlantısı kapatıldı")
        except Exception as e:
            logger.error(f"Mailer kapatılırken hata: {e}")

        # 2) Webhook kapat
        if self.webhook_runner:
            await self.webhook_runner.cleanup()
            logger.info("✅ Webhook runner temizlendi")

        # 3) Bot session kapat
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
        
        if config.webhook.USE_WEBHOOK:
            # Webhook modu
            logger.info("🚀 Webhook modu başlatıldı...")
            await server.start_webhook_mode()
            
            # Shutdown event'ini bekle
            await server.shutdown_event.wait()
        else:
            # Polling modu - shutdown event ile birlikte
            logger.info("🚀 Polling modu başlatıldı...")
            polling_task = asyncio.create_task(server.start_polling_mode())
            shutdown_task = asyncio.create_task(server.shutdown_event.wait())

            done, pending = await asyncio.wait(
                [shutdown_task, polling_task],
                return_when=asyncio.FIRST_COMPLETED
            )

            if shutdown_task in done:  # shutdown tetiklenmiş
                logger.info("📡 Shutdown sinyali geldi, polling task iptal ediliyor...")
                polling_task.cancel()
                try:
                    await polling_task
                except asyncio.CancelledError:
                    logger.info("📡 Polling task başarıyla iptal edildi")
                
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