# handlers/email_handler.py
"""
Toplu mail gönderim handler'ı
/toplumaile
/dosyalarıgöster
Input ve Output'taki tüm dosyaları ZIP yapar
ZIP'i PERSONAL_EMAIL'e gönderir
Input dosyasının ilk 6 karakterini ZIP ismi olarak kullanır
Dosya durumunu gösteren yardımcı komut
Reply keyboard desteği
Bu şekilde iki aşamalı işleminiz tamamlanmış olur!
"""
# handlers/email_handler.py - TAMAMEN DÜZELTİLMİŞ
import logging
import zipfile
import tempfile
from pathlib import Path
from aiogram import Router, types
from aiogram.filters import Command

from config import config
from utils.mailer import send_email_with_attachment

# Logger tanımla
logger = logging.getLogger(__name__)

router = Router(name="email_handlers")

@router.message(Command("toplumaile", "toplumail", "tmail"))
async def cmd_toplu_mail(message: types.Message):
    """Input ve Output dosyalarını ZIP yapıp PERSONAL_EMAIL'e gönderir"""
    try:
        await message.answer("📧 Input ve Output dosyaları ZIP yapılıp mail gönderiliyor...")
        
        # Klasör kontrolü
        if not await _check_directories_have_files():
            await message.answer("❌ Input veya Output klasörü boş. Önce /process komutu ile işlem yapın.")
            return
        
        # ZIP oluştur ve gönder
        success = await _create_and_send_zip()
        
        if success:
            await message.answer(
                f"✅ Input ve Output dosyaları başarıyla ZIP yapılıp gönderildi!\n"
                f"📧 Alıcı: {config.email.PERSONAL_EMAIL}"
            )
        else:
            await message.answer(f"❌ Mail gönderilemedi: {config.email.PERSONAL_EMAIL}")
            
    except Exception as e:
        logger.error(f"Toplu mail hatası: {e}")
        await message.answer("❌ İşlem sırasında hata oluştu.")

@router.message(Command("dosyalarıgöster", "dosyalar"))
async def cmd_dosyalari_goster(message: types.Message):
    """Input ve Output'taki dosyaları listeler"""
    try:
        response = await _generate_file_status_message()
        await message.answer(response)
        
    except Exception as e:
        logger.error(f"Dosya listeleme hatası: {e}")
        await message.answer("❌ Dosya listesi alınamadı.")

async def _check_directories_have_files() -> bool:
    """Input ve Output klasörlerinde dosya olup olmadığını kontrol eder"""
    input_has_files = any(config.paths.INPUT_DIR.iterdir())
    output_has_files = any(config.paths.OUTPUT_DIR.iterdir())
    return input_has_files or output_has_files

async def _create_and_send_zip() -> bool:
    """ZIP oluşturur ve mail gönderir"""
    zip_path = None
    try:
        zip_path = await _create_input_output_zip()
        if not zip_path:
            return False
        
        return await _send_zip_email(zip_path)
        
    except Exception as e:
        logger.error(f"ZIP oluşturma/gönderme hatası: {e}")
        return False
    finally:
        # Geçici ZIP'i temizle
        if zip_path and zip_path.exists():
            zip_path.unlink(missing_ok=True)

async def _create_input_output_zip() -> Path:
    """Input ve Output klasörlerindeki dosyaları ZIP yapar"""
    try:
        zip_name = await _generate_zip_name()
        zip_path = Path(tempfile.gettempdir()) / f"{zip_name}_toplu.zip"
        
        with zipfile.ZipFile(zip_path, 'w', zipfile.ZIP_DEFLATED) as zipf:
            # Input dosyalarını ekle
            for file_path in config.paths.INPUT_DIR.glob("*"):
                if file_path.is_file():
                    zipf.write(file_path, f"input/{file_path.name}")
            
            # Output dosyalarını ekle
            for file_path in config.paths.OUTPUT_DIR.glob("*"):
                if file_path.is_file():
                    zipf.write(file_path, f"output/{file_path.name}")
        
        return zip_path
        
    except Exception as e:
        logger.error(f"ZIP oluşturma hatası: {e}")
        return None

async def _generate_zip_name() -> str:
    """ZIP dosyası için isim oluşturur"""
    input_files = list(config.paths.INPUT_DIR.glob("*.xlsx"))
    if input_files:
        first_input = input_files[0]
        return first_input.stem[:6] if first_input.stem else "output_files"
    return "output_files"

async def _send_zip_email(zip_path: Path) -> bool:
    """ZIP dosyasını mail olarak gönderir"""
    if not config.email.PERSONAL_EMAIL:
        logger.error("PERSONAL_EMAIL tanımlı değil")
        return False
    
    try:
        subject = "📊 Rapor Tüm Dosyalar - emailh"
        body = (
            "Merhaba,\n\n"
            "Excel işleme sonucu oluşan tüm input ve output dosyaları ektedir.\n\n"
            "İyi çalışmalar,\nData_listesi_Hıdır"
        )
        
        return await send_email_with_attachment(
            [config.email.PERSONAL_EMAIL],
            subject,
            body,
            zip_path
        )
        
    except Exception as e:
        logger.error(f"ZIP mail gönderme hatası: {e}")
        return False

async def _generate_file_status_message() -> str:
    """Dosya durumu mesajını oluşturur"""
    input_files = list(config.paths.INPUT_DIR.glob("*"))
    output_files = list(config.paths.OUTPUT_DIR.glob("*"))
    
    response = ["📁 **DOSYA DURUMU**"]
    
    # Input dosyaları
    response.append("\n📥 **Input Dosyaları:**")
    if input_files:
        for file in input_files[:10]:
            size = file.stat().st_size / 1024
            response.append(f"• {file.name} ({size:.1f} KB)")
        if len(input_files) > 10:
            response.append(f"• ... ve {len(input_files) - 10} dosya daha")
    else:
        response.append("• Boş")
    
    # Output dosyaları
    response.append("\n📤 **Output Dosyaları:**")
    if output_files:
        for file in output_files[:10]:
            size = file.stat().st_size / 1024
            response.append(f"• {file.name} ({size:.1f} KB)")
        if len(output_files) > 10:
            response.append(f"• ... ve {len(output_files) - 10} dosya daha")
    else:
        response.append("• Boş")
    
    # Bilgilendirme
    response.append(f"\n📧 **Toplu Mail Alıcısı:** {config.email.PERSONAL_EMAIL}")
    response.append("\n🔗 **Komutlar:** /toplumaile - /dosyalarıgöster")
    
    return "\n".join(response)