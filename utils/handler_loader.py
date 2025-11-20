"""
Handler Loader Module
17-11-2025  14:20

Handler Loader Module uyum için handlerde bulunması gerekenler
Handler - Aiogram 3.x uyumlu olacak
router = Router() - Bu satır mutlaka olmalı
@router.message() veya @router.callback_query() decorator'ları
Async handler fonksiyonları

örnek:
from aiogram import Router
from aiogram import types
from aiogram.filters import Command
router = Router(name="module_name")  # Debug kolaylığı

@router.message(Command("start"))
async def cmd_start(message: types.Message):
    await message.answer("Merhaba!")
    
--ZORUNLU GEREKENLER--
Gereken Özellik	Açıklama	Örnek
router değişkeni	aiogram.Router örneği olmalı	router = Router()
Decorator formatı	@router.message(...) veya @router.callback_query(...)	@router.message(Command("start"))
Fonksiyon tipi	Async fonksiyon olmalı	async def handler(...)
Aiogram sürümü	3.x Router yapısı	

--ihtiyaca göre akıllıca kullanılacak--
gereksiz yere  Logger  ve Type Hints kullanma.
Logger = Production odaklı → Dosya bazlı + sadece kritik event'ler
Type Hints = Development odaklı → Complex fonksiyonlar + takım projeleri

# KOVA HANDLER LOADER
"""

# handler_loader.py
import time
import os
import asyncio
import logging
import importlib
import inspect
import sys
import importlib.util  
from typing import Dict, List, Any, Optional, Set, Type, Callable
from pathlib import Path
from dataclasses import dataclass
from types import ModuleType
from contextlib import contextmanager

from aiogram import Dispatcher, Router

# Configure logger - get_context_logger yerine standart logger
logger = logging.getLogger(__name__)

@dataclass
class HandlerLoadResult:
    """Handler loading result with detailed metrics."""
    loaded: int = 0
    failed: int = 0
    skipped: int = 0
    total_files: int = 0
    errors: List[str] = None
    loaded_handlers: List[str] = None
    
    def __post_init__(self):
        if self.errors is None:
            self.errors = []
        if self.loaded_handlers is None:
            self.loaded_handlers = []
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert result to dictionary for logging."""
        return {
            'loaded': self.loaded,
            'failed': self.failed,
            'skipped': self.skipped,
            'total_files': self.total_files,
            'errors': self.errors,
            'loaded_handlers': self.loaded_handlers
        }

class HandlerCache:
    """Simplified handler cache without excessive locking."""
    
    def __init__(self):
        self._loaded_modules: Set[str] = set()
    
    def is_module_loaded(self, module_path: str) -> bool:
        """Check if module is already loaded."""
        return module_path in self._loaded_modules
    
    def mark_module_loaded(self, module_path: str):
        """Mark module as loaded."""
        self._loaded_modules.add(module_path)
    
    def clear_cache(self):
        """Clear handler cache."""
        self._loaded_modules.clear()
        logger.info("✅ Handler cache cleared")

class HandlerLoader:
    """
    Optimized dynamic handler loader with reduced redundancy.
    """
    
    def __init__(self, dispatcher: Dispatcher, base_path: str = "handlers", 
                 handler_dirs: Optional[List[str]] = None):
        self.dispatcher = dispatcher
        self.base_path = Path(base_path)
        self.cache = HandlerCache()
        
        self.handler_dirs = handler_dirs or [
            "commands", "callbacks", "messages", "errors", "admin"
        ]
        
        logger.info(f"🔄 HandlerLoader initialized with base path: {base_path}")

    async def load_handlers(self, dispatcher: Dispatcher, retry_count: int = 0) -> Dict[str, int]:
        """
        Load all handlers dynamically with retry mechanism.
        """
        result = HandlerLoadResult()
        
        try:
            if retry_count == 0:
                self.cache.clear_cache()
            
            if not await self._ensure_handlers_directory(result, retry_count):
                return result.to_dict()
            
            await self._load_all_handler_directories(dispatcher, result)
            await self._load_root_handlers(dispatcher, result)
            
            logger.info(
                f"✅ Handler loading completed: "
                f"{result.loaded} loaded, {result.failed} failed, "
                f"{result.skipped} skipped"
            )
            
        except Exception as e:
            logger.error(f"❌ Critical error in handler loading: {e}")
            result.failed += 1
            result.errors.append(f"Critical error: {str(e)}")
        
        return result.to_dict()

    async def _ensure_handlers_directory(self, result: HandlerLoadResult, retry_count: int) -> bool:
        """Ensure handlers directory exists with safe retry mechanism"""
        if self.base_path.exists():
            return True
        
        logger.warning(f"⚠️ Handlers directory not found: {self.base_path}")
        
        if retry_count >= 2:
            logger.error(f"💥 Max retry count reached for directory creation: {self.base_path}")
            result.errors.append("Max retry count reached for directory creation")
            return False
        
        try:
            success = await self._create_directory_structure()
            return success
                
        except Exception as e:
            logger.error(f"❌ Error creating handlers directory: {e}")
            result.errors.append(f"Directory creation error: {str(e)}")
            return False

    async def _load_all_handler_directories(self, dispatcher: Dispatcher, result: HandlerLoadResult) -> None:
        """Load handlers from all configured directories"""
        for handler_dir in self.handler_dirs:
            dir_path = self.base_path / handler_dir
            
            if not await self._ensure_directory_exists(dir_path, f"handler directory: {handler_dir}"):
                continue
            
            await self._load_handlers_from_directory(dir_path, handler_dir, result, dispatcher)

    async def _load_root_handlers(self, dispatcher: Dispatcher, result: HandlerLoadResult) -> None:
        """Load handlers from root handlers directory"""
        root_files = list(self.base_path.glob("*.py"))
        
        if root_files:
            logger.info("📁 Loading root handlers...")
            await self._load_handlers_from_directory(self.base_path, "root", result, dispatcher)

    async def _create_directory_structure(self) -> bool:
        """Create directory structure with async file operations"""
        logger.info("🏗️ Creating default handler directory structure...")
        
        try:
            # Sync directory creation (hızlı işlemler)
            self.base_path.mkdir(exist_ok=True)
            
            for handler_dir in self.handler_dirs:
                dir_path = self.base_path / handler_dir
                dir_path.mkdir(exist_ok=True)
                await self._create_init_file(dir_path)
            
            await self._create_init_file(self.base_path)
            await self._create_example_handlers()
            
            logger.info("✅ Default handler directory structure created successfully")
            return True
            
        except Exception as e:
            logger.error(f"❌ Failed to create handler directory structure: {e}")
            return False

    async def _create_init_file(self, directory: Path) -> None:
        """Create __init__.py file asynchronously"""
        init_file = directory / "__init__.py"
        if not init_file.exists():
            await asyncio.to_thread(init_file.write_text, '"""Handler module."""\n')

    async def _ensure_directory_exists(self, dir_path: Path, description: str) -> bool:
        """Ensure directory exists with proper initialization"""
        if dir_path.exists():
            return True
            
        try:
            logger.debug(f"📁 Creating missing {description}: {dir_path}")
            dir_path.mkdir(parents=True, exist_ok=True)
            await self._create_init_file(dir_path)
            return True
        except Exception as e:
            logger.warning(f"⚠️ Could not create {description} {dir_path}: {e}")
            return False

    async def _create_example_handlers(self) -> None:
        """Create example handler files using async file operations"""
        example_handlers = {
            self.base_path / "commands" / "start.py": '''"""
Example start command handler.
"""
from aiogram import Router, types
from aiogram.filters import Command

router = Router(name="start_commands")

@router.message(Command("start"))
async def cmd_start(message: types.Message):
    """Start command handler."""
    await message.answer("👋 Hello! Bot is working!")

@router.message(Command("help"))
async def cmd_help(message: types.Message):
    """Help command handler."""
    await message.answer("ℹ️ This is a help message!")
''',
            self.base_path / "callbacks" / "main.py": '''"""
Example callback handler.
"""
from aiogram import Router, types

router = Router(name="main_callbacks")

@router.callback_query(lambda c: c.data == "test")
async def handle_test_callback(callback: types.CallbackQuery):
    """Example callback handler."""
    await callback.answer("Test callback worked!")
    await callback.message.answer("✅ Callback processed!")
'''
        }
        
        for file_path, content in example_handlers.items():
            if not file_path.exists():
                try:
                    file_path.parent.mkdir(parents=True, exist_ok=True)
                    await asyncio.to_thread(file_path.write_text, content)
                except Exception as e:
                    logger.debug(f"Note: Could not create example handler {file_path}: {e}")

    async def _load_handlers_from_directory(self, dir_path: Path, category: str, 
                                          result: HandlerLoadResult, dispatcher: Dispatcher):
        """Load handlers from a specific directory."""
        if not dir_path.exists():
            logger.debug(f"📁 Handler directory not found, skipping: {dir_path}")
            return
        
        logger.info(f"📁 Loading handlers from: {category}")
        
        py_files = list(dir_path.glob("*.py"))
        result.total_files += len(py_files)
        
        for py_file in py_files:
            if py_file.name.startswith("__"):
                continue
                
            module_name = f"handlers.{category}.{py_file.stem}"
            await self._load_handler_module(module_name, py_file, result, dispatcher)

    async def _load_handler_module(self, module_name: str, file_path: Path, 
                                 result: HandlerLoadResult, dispatcher: Dispatcher) -> None:
        """Load and register handlers from a module"""
        try:
            if self.cache.is_module_loaded(module_name):
                logger.debug(f"📦 Module already loaded, skipping: {module_name}")
                result.skipped += 1
                return

            module = await self._load_module_with_logger(module_name, file_path, result)
            if module is None:
                return

            await self._register_router(module, module_name, result, dispatcher)

        except Exception as e:
            logger.error(f"❌ Error loading {module_name}: {e}")
            result.failed += 1
            result.errors.append(f"Loading error: {module_name} - {str(e)}")

    async def _load_module_with_logger(self, module_name: str, file_path: Path, 
                                     result: HandlerLoadResult) -> Optional[ModuleType]:
        """Load module with safe logger injection"""
        try:
            # Clear old module
            if module_name in sys.modules:
                old_module = sys.modules[module_name]
                if hasattr(old_module, 'router'):
                    await self._cleanup_router(old_module.router)
                del sys.modules[module_name]

            spec = importlib.util.spec_from_file_location(module_name, file_path)
            if spec is None or spec.loader is None:
                logger.warning(f"❌ Could not load spec for: {module_name}")
                result.failed += 1
                result.errors.append(f"Spec loading failed: {module_name}")
                return None

            module = importlib.util.module_from_spec(spec)
            sys.modules[module_name] = module
            
            # Inject logger safely - get_context_logger olmadan
            await self._inject_logger_to_module(module, module_name)
            
            # Load module
            with self._timer(f"Module load: {module_name}"):
                spec.loader.exec_module(module)
            
            logger.debug(f"✅ Module loaded successfully: {module_name}")
            return module

        except Exception as e:
            logger.error(f"❌ Module loading failed for {module_name}: {e}")
            result.failed += 1
            result.errors.append(f"Module loading failed: {module_name} - {str(e)}")
            
            if module_name in sys.modules:
                del sys.modules[module_name]
            return None

    async def _inject_logger_to_module(self, module: ModuleType, module_name: str) -> None:
        """Safely inject logger into module - get_context_logger olmadan"""
        try:
            # Basit logger injection - context logger olmadan
            import logging
            module.logger = logging.getLogger(module_name)
        except Exception as e:
            logger.warning(f"⚠️ Logger injection failed for {module_name}: {e}")
            import logging
            module.logger = logging.getLogger(module_name)

    async def _register_router(self, module: ModuleType, module_name: str,
                             result: HandlerLoadResult, dispatcher: Dispatcher) -> None:
        """Unified router registration"""
        if not hasattr(module, 'router'):
            logger.debug(f"⏭️ No router found in {module_name}")
            result.skipped += 1
            return

        router = module.router
        
        if not isinstance(router, Router):
            logger.error(f"❌ 'router' is not a Router instance in {module_name}")
            result.failed += 1
            result.errors.append(f"Router is not instance: {module_name}")
            return

        try:
            if await self._is_router_attached(router, dispatcher):
                logger.warning(f"⚠️ Router already attached, skipping: {module_name}")
                result.skipped += 1
                return

            dispatcher.include_router(router)
            self.cache.mark_module_loaded(module_name)
            result.loaded += 1
            result.loaded_handlers.append(module_name)
            
            handler_count = await self._count_router_handlers(router)
            router_name = getattr(router, 'name', 'unnamed')
            logger.info(f"✅ Loaded router '{router_name}' with {handler_count} handlers from {module_name}")

        except Exception as e:
            logger.error(f"❌ Router registration error for {module_name}: {e}")
            result.failed += 1
            result.errors.append(f"Router registration failed: {module_name} - {str(e)}")

    async def _is_router_attached(self, router: Router, dispatcher: Dispatcher) -> bool:
        """Check if router is already attached to dispatcher"""
        return hasattr(dispatcher, '_routers') and router in dispatcher._routers

    async def _cleanup_router(self, router: Router) -> None:
        """Cleanup router connections"""
        try:
            if hasattr(router, '_parent_router'):
                router._parent_router = None
        except Exception:
            pass

    async def _count_router_handlers(self, router: Router) -> int:
        """Count handlers in router"""
        try:
            return await self._count_handlers_recursive(router)
        except Exception:
            return 1

    async def _count_handlers_recursive(self, router: Router) -> int:
        """Recursively count handlers in router and sub-routers"""
        count = 0
        
        if hasattr(router, '_handlers'):
            for handler in router._handlers:
                if hasattr(handler, 'handlers'):
                    count += len(handler.handlers)
        
        if hasattr(router, 'sub_routers'):
            for sub_router in router.sub_routers:
                count += await self._count_handlers_recursive(sub_router)
        
        return count or 1

    @contextmanager
    def _timer(self, operation: str):
        """Performance timer context manager"""
        start = time.time()
        try:
            yield
        finally:
            logger.debug(f"⏱️ {operation} took {time.time() - start:.3f}s")
            
# not: (EmergencyHandlerLoader ve singleton functions gerek yok)
