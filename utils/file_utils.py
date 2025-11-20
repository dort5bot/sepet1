#Yardımcı Dosya İşlemleri (utils/file_utils.py)
"""
20-11-25
Bu status modülü > admin_handler içinde, botun durumunu izlemek, 
logları görüntülemek, istatistikleri toplamak ve 
# Yaptığı işler:
- Son işlenen dosyaları listeler
- Dosya istatistikleri oluşturur (toplam işlenen, başarılı/başarısız)
- Bellek kullanımı ve disk boyutlarını hesaplar
- Dizin tarama ve dosya bilgisi toplama
- Async dosya operasyonları

"""

# file_utils.py
import os
import platform  
from datetime import datetime
from pathlib import Path
import psutil
import aiofiles
import aiofiles.os
import asyncio
from config import config
from utils.logger import logger

async def get_recent_processed_files(limit: int = 10):
    """Output klasöründen son işlenen dosyaları async döndürür"""
    files = []
    # ✅ DÜZELTİLDİ: config.paths.OUTPUT_DIR
    output_dir = config.paths.OUTPUT_DIR

    if not await aiofiles.os.path.exists(output_dir):
        return []

    # Async dosya listeleme
    async for file_path in _async_glob(output_dir, "*.xlsx"):
        try:
            stat = await aiofiles.os.stat(file_path)
            files.append({
                "name": file_path.name,
                "size": f"{stat.st_size / 1024:.1f} KB",
                "modified": datetime.fromtimestamp(stat.st_mtime)
            })
            if len(files) >= limit:
                break
        except Exception as e:
            continue

    # Zaman sıralama
    files.sort(key=lambda x: x["modified"], reverse=True)
    return files[:limit]

async def get_system_stats() -> dict:
    """Sistem istatistiklerini dict olarak döndürür"""
    try:
        memory = psutil.virtual_memory()
        disk = psutil.disk_usage('/')
        
        return {
            "cpu_percent": psutil.cpu_percent(interval=0.1),
            "memory_percent": memory.percent,
            "disk_percent": disk.percent,
            "memory_used_gb": memory.used / (1024**3),
            "disk_used_gb": disk.used / (1024**3),
            # ✅ DÜZELTİLDİ: platform import edildi
            "platform": f"{platform.system()} {platform.release()}"
        }
    except Exception as e:
        logger.error(f"Sistem istatistikleri hatası: {e}")
        return {}

# 
async def get_group_stats() -> dict:
    """Grup istatistiklerini döndürür"""
    try:
        from utils.group_manager import group_manager
        groups = group_manager.groups.get("groups", [])
        
        return {
            "total_groups": len(groups),
            "total_cities": sum(len(group.get("cities", [])) for group in groups),
            "total_emails": sum(len(group.get("email_recipients", [])) for group in groups)
        }
    except Exception as e:
        logger.error(f"Grup istatistikleri hatası: {e}")
        return {"total_groups": 0, "total_cities": 0, "total_emails": 0}



async def get_directory_size(path: Path) -> str:
    """Optimize edilmiş dizin boyutu hesaplama"""
    if not await aiofiles.os.path.exists(path):
        return "0.00 MB"
    
    total_size = 0
    try:
        async for file_path in _async_glob(path, "*"):
            if await aiofiles.os.path.isfile(file_path):
                try:
                    stat = await aiofiles.os.stat(file_path)
                    total_size += stat.st_size
                except OSError:
                    continue
        
        # Daha okunabilir format
        if total_size < 1024 * 1024:  # KB
            return f"{total_size / 1024:.1f} KB"
        elif total_size < 1024 * 1024 * 1024:  # MB
            return f"{total_size / (1024 * 1024):.1f} MB"
        else:  # GB
            return f"{total_size / (1024 * 1024 * 1024):.1f} GB"
            
    except Exception as e:
        logger.error(f"Dizin boyutu hesaplama hatası {path}: {e}")
        return "Hesaplanamadı"

async def _async_glob(directory, pattern):
    import glob
    files = await asyncio.to_thread(glob.glob, str(directory / pattern))
    for file_path in files:
        yield Path(file_path)
        
