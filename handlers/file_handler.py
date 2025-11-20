# handlers/file_handler.py
"""
20/11/25
Dosya Yönetim Handlers
/files o → Output dosyalarını zip olarak indirir
/files l → Log dosyalarını zip olarak indirir
/clear → Input, Output, Groups ve temp dosyalarını temizler
/clear log → Sadece log dosyalarını temizler
"""
import os
import shutil
import zipfile
import tempfile
from pathlib import Path
from aiogram import Router, types
from aiogram.filters import Command
from config import config

router = Router(name="file_handlers")

class FileManager:
    """Dosya yönetimi için merkezi sınıf"""
    
    @staticmethod
    async def create_zip_archive(files_dir: Path, archive_name: str) -> Path:
        """Dosyaları zip arşivi olarak paketle"""
        if not files_dir.exists() or not any(files_dir.iterdir()):
            raise ValueError(f"Klasör boş veya mevcut değil: {files_dir}")
        
        zip_path = Path(tempfile.gettempdir()) / archive_name
        
        with zipfile.ZipFile(zip_path, 'w', zipfile.ZIP_DEFLATED) as zipf:
            for file_path in files_dir.glob('*'):
                if file_path.is_file():
                    zipf.write(file_path, file_path.name)
        
        return zip_path

    @staticmethod
    async def cleanup_directory(directory: Path, keep: list = None) -> tuple[int, int]:
        """Klasördeki dosyaları temizle ve istatistik döndür"""
        keep = keep or []

        cleared_files = 0
        cleared_size = 0
        
        if directory.exists():
            for file_path in directory.glob('*'):
                if file_path.name in keep:
                    continue

                if file_path.is_file():
                    try:
                        file_size = file_path.stat().st_size
                        file_path.unlink()
                        cleared_files += 1
                        cleared_size += file_size
                    except Exception:
                        continue
        
        return cleared_files, cleared_size

    @staticmethod
    async def cleanup_temp_files() -> tuple[int, int]:
        """Geçici dosyaları temizle"""
        cleared_files = 0
        cleared_size = 0
        temp_dir = Path(tempfile.gettempdir())
        
        temp_patterns = ['*.xlsx', '*.xls', '*.tmp', '*_files_*.zip']
        
        for pattern in temp_patterns:
            for file_path in temp_dir.glob(pattern):
                if file_path.is_file():
                    try:
                        file_size = file_path.stat().st_size
                        file_path.unlink()
                        cleared_files += 1
                        cleared_size += file_size
                    except Exception:
                        continue
        
        return cleared_files, cleared_size

@router.message(Command("files"))
async def cmd_files(message: types.Message):
    """Dosya yönetimi komutları"""
    args = message.text.strip().split()[1:]
    mode = args[0].lower() if args else ""
    
    if mode == "o":
        await download_output_files(message)
    elif mode == "l":
        await download_log_files(message)
    else:
        await show_files_help(message)

@router.message(Command("clear"))
async def cmd_clear(message: types.Message):
    """Temizlik komutları"""
    args = message.text.strip().split()[1:]
    mode = args[0].lower() if args else ""
    
    if mode == "log":
        await clear_logs(message)
    else:
        await clear_all(message)

async def show_files_help(message: types.Message):
    """Dosya komutları yardım mesajı"""
    help_text = (
        "📁 Dosya Yönetimi Komutları:\n\n"
        "/files o → Output dosyalarını indir\n"
        "/files l → Log dosyalarını indir\n\n"
        "🧹 Temizlik Komutları:\n"
        "/clear → Input/Output/Groups/temp dosyalarını temizler\n"
        "/clear log → Sadece log dosyalarını temizler"
    )
    await message.answer(help_text)

async def download_output_files(message: types.Message):
    """Output dosyalarını zip olarak indir"""
    try:
        user_id = message.from_user.id
        zip_path = await FileManager.create_zip_archive(
            config.OUTPUT_DIR, 
            f"output_files_{user_id}.zip"
        )
        
        await message.answer_document(
            types.FSInputFile(zip_path),
            caption="📁 Output dosyaları"
        )
        
        zip_path.unlink(missing_ok=True)
        
    except ValueError:
        await message.answer("❌ Output klasörü boş veya mevcut değil.")
    except Exception:
        await message.answer("❌ Output dosyaları indirilemedi.")

async def download_log_files(message: types.Message):
    """Log dosyalarını zip olarak indir"""
    try:
        user_id = message.from_user.id
        zip_path = await FileManager.create_zip_archive(
            config.LOGS_DIR, 
            f"log_files_{user_id}.zip"
        )
        
        await message.answer_document(
            types.FSInputFile(zip_path),
            caption="📝 Log dosyaları"
        )
        
        zip_path.unlink(missing_ok=True)
        
    except ValueError:
        await message.answer("❌ Log klasörü boş veya mevcut değil.")
    except Exception:
        await message.answer("❌ Log dosyaları indirilemedi.")

async def clear_all(message: types.Message):
    """Output, Input, Groups ve temp temizliği"""
    try:
        total_files = 0
        total_size = 0
        
        directories = [
            (config.paths.INPUT_DIR, "Input", []),
            (config.paths.OUTPUT_DIR, "Output", []),
            # (config.paths.LOGS_DIR, "Output", []),    #LOGS dosyasınıda silmek istersen
            (config.paths.GROUPS_DIR, "Groups", ["groups.json"])
        ]
        
        cleared_dirs = []
        for directory, name, keep in directories:
            files, size = await FileManager.cleanup_directory(directory, keep=keep)
            total_files += files
            total_size += size
            if files > 0:
                cleared_dirs.append(name)
                
        temp_files, temp_size = await FileManager.cleanup_temp_files()
        total_files += temp_files
        total_size += temp_size
        if temp_files > 0:
            cleared_dirs.append("Geçici dosyalar")
        
        cleared_size_mb = total_size / (1024 * 1024)
        
        if total_files > 0:
            result_text = (
                f"🧹 Temizlik tamamlandı!\n\n"
                f"• Silinen dosya: {total_files}\n"
                f"• Kazanılan alan: {cleared_size_mb:.2f} MB\n\n"
                f"Temizlenen klasörler:\n• " + "\n• ".join(cleared_dirs)
            )
        else:
            result_text = "✅ Temizlenecek dosya bulunamadı."
        
        await message.answer(result_text)
        
    except Exception:
        await message.answer("❌ Temizlik işlemi başarısız oldu.")

async def clear_logs(message: types.Message):
    """Sadece log dosyalarını temizle"""
    try:
        cleared_files, cleared_size = await FileManager.cleanup_directory(
            config.paths.LOGS_DIR, 
            keep=[]
        )
        
        if cleared_files > 0:
            cleared_size_mb = cleared_size / (1024 * 1024)
            result_text = (
                f"📝 Log temizliği tamamlandı!\n\n"
                f"• Silinen dosya: {cleared_files}\n"
                f"• Kazanılan alan: {cleared_size_mb:.2f} MB"
            )
        else:
            result_text = "✅ Temizlenecek log dosyası bulunamadı."
        
        await message.answer(result_text)
        
    except Exception:
        await message.answer("❌ Log temizleme işlemi başarısız oldu.")