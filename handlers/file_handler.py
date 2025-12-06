# handlers/file_handler.py
"""
/file o → Output dosyalarını zip olarak indir
/file l → Log dosyalarını zip olarak indir
/file c → Input/Output/Groups/temp dosyalarını temizler
repley burdan temizlik komutunu çağırır

/file c l → Sadece log dosyalarını temizler
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
    @staticmethod
    async def create_zip_archive(files_dir: Path, archive_name: str) -> Path:
        if not files_dir.exists() or not any(files_dir.iterdir()):
            raise ValueError(f"Klasör boş veya mevcut değil: {files_dir}")
        
        zip_path = Path(tempfile.gettempdir()) / archive_name
        
        with zipfile.ZipFile(zip_path, 'w', zipfile.ZIP_DEFLATED) as zipf:
            for file_path in files_dir.glob('*'):
                if file_path.is_file():
                    zipf.write(file_path, file_path.name)
        
        return zip_path

    @staticmethod
    async def cleanup_directory(directory: Path, keep: list = None, recursive: bool = True) -> tuple[int, int]:
        keep = keep or []
        cleared_files = 0
        cleared_size = 0
        
        if not directory.exists():
            return cleared_files, cleared_size
        
        # Hem dizindeki hem de alt dizinlerdeki dosyaları kontrol et
        if recursive:
            file_iterator = directory.rglob('*')
        else:
            file_iterator = directory.glob('*')
        
        for item_path in file_iterator:
            # Sadece dosyaları kontrol et (dizinleri atla)
            if not item_path.is_file():
                continue
                
            # Keep listesindeki dosyaları atla
            if item_path.name in keep:
                continue
            
            try:
                file_size = item_path.stat().st_size
                item_path.unlink()
                cleared_files += 1
                cleared_size += file_size
            except Exception as e:
                print(f"Silinemedi {item_path}: {e}")
                continue
        
        return cleared_files, cleared_size
    
    
    @staticmethod
    async def cleanup_temp_files() -> tuple[int, int]:
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

@router.message(Command("file"))
async def cmd_files(message: types.Message):
    """Tüm dosya işlemleri tek komutta"""
    args = message.text.strip().split()[1:]
    
    if not args:
        await show_files_help(message)
        return
    
    mode = args[0].lower()
    sub_mode = args[1].lower() if len(args) > 1 else ""
    
    if mode == "o":
        await download_output_files(message)
    elif mode == "l":
        await download_log_files(message)
    elif mode == "c":
        if sub_mode == "l":
            await clear_logs(message)
        else:
            await clear_all(message)
    else:
        await show_files_help(message)

async def show_files_help(message: types.Message):
    """Yeni yardım mesajı"""
    help_text = (
        "📁 DOSYA YÖNETİMİ - Tek Komut\n\n"
        "📥 İNDİRME:\n"
        "/file o → Output dosyalarını indir (zip)\n"
        "/file l → Log dosyalarını indir (zip)\n\n"
        "🧹 TEMİZLİK:\n"
        "/file c → Tüm dosyaları temizle\n"
        "/file c l → Sadece logları temizle\n\n"
        "📝 Not: Tüm işlemler tek /file komutu altında!"
    )
    await message.answer(help_text)

async def download_output_files(message: types.Message):
    """Output dosyalarını zip olarak indir"""
    try:
        user_id = message.from_user.id
        zip_path = await FileManager.create_zip_archive(
            config.paths.OUTPUT_DIR,
            f"output_files_{user_id}.zip"
        )
        await message.answer_document(
            types.FSInputFile(zip_path),
            caption="📁 Output dosyaları (zip)"
        )
        
        zip_path.unlink(missing_ok=True)
        
    except ValueError as e:
        await message.answer(f"❌ Output klasörü boş: {str(e)}")
    except Exception as e:
        await message.answer(f"❌ İndirme başarısız: {str(e)}")

async def download_log_files(message: types.Message):
    """Log dosyalarını zip olarak indir"""
    try:
        user_id = message.from_user.id
        zip_path = await FileManager.create_zip_archive(
            config.paths.LOGS_DIR,
            f"log_files_{user_id}.zip"
        )
        
        await message.answer_document(
            types.FSInputFile(zip_path),
            caption="📝 Log dosyaları (zip)"
        )
        
        zip_path.unlink(missing_ok=True)
        
    except ValueError as e:
        await message.answer(f"❌ Log klasörü boş: {str(e)}")
    except Exception as e:
        await message.answer(f"❌ İndirme başarısız: {str(e)}")

async def clear_all(message: types.Message):
    """Tüm dosyaları temizle"""
    try:
        total_files = 0
        total_size = 0
        
        directories = [
            (config.paths.INPUT_DIR, "Input", []),
            (config.paths.OUTPUT_DIR, "Output", []),
            # Loglarda sadece bot.log ve errors.log korunacak
            (config.paths.LOGS_DIR, "Logs", ["bot.log", "errors.log"]),
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
                f"🧹 TEMİZLİK TAMAMLANDI!\n\n"
                f"• ana dosyalar dışında herşey silinir\n"
                f"• Silinen dosya: {total_files}\n"
                f"• Kazanılan alan: {cleared_size_mb:.2f} MB\n"
                f"• Klasörler: {' | '.join(cleared_dirs)}"
            )
        else:
            result_text = "✅ Temizlenecek dosya yok."
        
        await message.answer(result_text)
        
    except Exception as e:
        await message.answer(f"❌ Temizlik başarısız: {str(e)}")


"""async def clear_logs(message: types.Message):
    try:
        cleared_files, cleared_size = await FileManager.cleanup_directory(
            config.paths.LOGS_DIR,
            keep=[],        # Tüm loglar silinecek (bot.log ve errors.log dahil)
            recursive=True  # Alt dizinleri de temizle
        )
        
        if cleared_files > 0:
            cleared_size_mb = cleared_size / (1024 * 1024)
            result_text = (
                f"📝 LOG TEMİZLİĞİ TAMAMLANDI!\n\n"
                f"• Silinen log: {cleared_files}\n"
                f"• Kazanılan alan: {cleared_size_mb:.2f} MB"
            )
        else:
            # Log dizinindeki dosyaları kontrol et
            log_dir = config.paths.LOGS_DIR
            all_files = list(log_dir.rglob('*.*'))
            if all_files:
                file_list = "\n".join([f"- {f.name}" for f in all_files if f.is_file()])
                result_text = f"✅ Loglar temizlendi veya boş. Mevcut dosyalar:\n{file_list}"
            else:
                result_text = "✅ Log klasörü boş."
        
        await message.answer(result_text)
        
    except Exception as e:
        await message.answer(f"❌ Log temizleme başarısız: {str(e)}")
"""

# Sadece log dosyalarının içini temizle (truncate)
# dosya silinmesi tehlikelidir

async def clear_logs(message: types.Message):
    """Sadece log dosyalarının içini temizle (truncate)"""
    try:
        log_dir = config.paths.LOGS_DIR
        
        # Sadece .log dosyalarını bul
        log_files = [f for f in log_dir.glob("*.log") if f.is_file()]
        
        if not log_files:
            await message.answer("ℹ️ Temizlenecek log dosyası bulunamadı.")
            return
        
        cleared = []
        for file_path in log_files:
            try:
                # İçeriği sıfırla (truncate)
                file_path.write_text("")
                cleared.append(file_path.name)
            except Exception as e:
                print(f"Log temizlenemedi {file_path}: {e}")
        
        if cleared:
            result_text = (
                "📝 LOG DOSYALARI SIFIRLANDI!\n\n"
                "Temizlenen loglar:\n" +
                "\n".join(f"- {name}" for name in cleared)
            )
        else:
            result_text = "ℹ️ Temizlenecek log dosyası yok."
        
        await message.answer(result_text)
    
    except Exception as e:
        await message.answer(f"❌ Log temizleme hatası: {str(e)}")
