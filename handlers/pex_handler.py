# handlers/pex_handler.py
"""
PEX Handler Module - GÜNCELLENMİŞ VERSİYON
Dosya adı bazlı dağıtım işlemleri (ZIP'siz doğrudan gönderim)

version: 27-11-2025
"""
from pathlib import Path
from typing import Dict, List
from aiogram import Router, F
from aiogram.types import Message
from aiogram.filters import Command
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup

from config import config
from utils.group_manager import group_manager
from utils.mailer import send_email_with_multiple_attachments, send_simple_email
from utils.logger import logger

# Handler loader uyumlu router tanımı
router = Router(name="pex_processor")

class PexProcessingStates(StatesGroup):
    """PEX işleme state'leri"""
    waiting_for_files = State()

@router.message(Command("pex"))
async def cmd_pex(message: Message, state: FSMContext):
    """PEX - Dosya adı bazlı dağıtım komutu"""
    await state.set_state(PexProcessingStates.waiting_for_files)
    await message.answer(
        "📁 **PEX MODU - DOSYA ADI BAZLI DAĞITIM**\n\n"
        "Lütfen dağıtmak istediğiniz dosyaları gönderin.\n\n"
        "📋 **KURALLAR:**\n"
        "• Dosya adı şehir adı olmalı: 'ankara.pdf', 'van.xlsx' gibi\n"
        "• Desteklenen formatlar: PDF, Excel (.xls, .xlsx)\n"
        "• Dosyaları TEK TEK gönder(en iyisi bu yöntem)\n\n"
        "🔄 **İŞLEM:**\n"
        "1. Dosya adındaki şehir gruplarda aranır\n"
        "2. Eşleşen tüm gruplara dosya gönderilir\n"
        "3. Her grup kendi email listesine ulaşır\n\n"
        "📤 **DOSYA BEKLİYORUM...**\n"
        "Lütfen PDF veya Excel dosyası gönderin.\n\n"
        "🛑 İptal etmek için '/iptal' komutunu kullanın veya DUR butonuna basın."
    )

@router.message(PexProcessingStates.waiting_for_files, F.document)
async def handle_pex_file_upload(message: Message, state: FSMContext):
    """PEX dosyalarını işler"""
    # Dosya formatı kontrolü
    valid_extensions = {'.pdf', '.xls', '.xlsx'}
    file_ext = Path(message.document.file_name).suffix.lower()
    
    if file_ext not in valid_extensions:
        await message.answer("❌ Desteklenmeyen dosya formatı. PDF veya Excel gönderin.")
        return
    
    try:
        # Dosya adından şehir adını çıkar
        city_name = Path(message.document.file_name).stem.lower()
        
        # Mevcut state'deki dosyaları al
        current_data = await state.get_data()
        pex_files = current_data.get('pex_files', [])
        
        # Dosyayı indir 
        file_info = await message.bot.get_file(message.document.file_id)
        file_path = config.paths.INPUT_DIR / message.document.file_name
        
        await message.bot.download_file(file_info.file_path, file_path)
        
        # Dosya bilgisini kaydet
        pex_files.append({
            'path': file_path,
            'filename': message.document.file_name,
            'city_name': city_name,
            'extension': file_ext
        })
        
        await state.update_data(pex_files=pex_files)
        
        await message.answer(
            f"✅ Dosya eklendi: {message.document.file_name}\n"
            f"🏙️  Algılanan şehir: {city_name.upper()}\n"
            f"📁 Toplam dosya: {len(pex_files)}\n\n"
            "📤 *DOSYA BEKLİYORUM...*\n\n"
            "Dosya varsa ekle, işlemi başlatmak için '/tamam' yazın.\n\n"
            "🛑 İptal için '/iptal' veya DUR butonu"
        )
        
    except Exception as e:
        logger.error(f"PEX dosya işleme hatası: {e}")
        await message.answer("❌ Dosya işlenirken hata oluştu.")


# handle_process_pex fonksiyonundaki mail gönderim kısmını değiştirin
@router.message(PexProcessingStates.waiting_for_files, F.text == "/tamam")
async def handle_process_pex(message: Message, state: FSMContext):
    """PEX işlemini başlat - (RAPOR MAILI EKLENDİ)"""
    data = await state.get_data()
    pex_files = data.get('pex_files', [])
    
    if not pex_files:
        await message.answer("❌ İşlenecek dosya yok.")
        await state.clear()
        return
    
    await message.answer("⏳ Dosyalar gruplara dağıtılıyor ve mailler hazırlanıyor...")
    
    try:
        # 1. Gruplara dağıtım (TEK MAIL - ÇOKLU DOSYA)
        result = await _process_pex_distribution(pex_files)
        
        # 2. Input email'e TÜM DOSYALARI TEK MAIL olarak gönder
        input_email_sent = False
        if pex_files and config.email.INPUT_EMAIL:
            input_email_sent = await _send_all_files_to_input_email(pex_files)
        
        if result["success"]:
            report = await _generate_pex_report(result, input_email_sent, len(pex_files))
            await message.answer(report)
            
            # 2. Raporu PERSONAL_EMAIL'e gönder (DÜZELTİLDİ)
            if config.email.PERSONAL_EMAIL:
                await send_simple_email(
                    [config.email.PERSONAL_EMAIL],
                    f"📊 PEX Raporu - {len(pex_files)} Dosya",
                    report
                )
                await message.answer("✅ Rapor PERSONAL_EMAIL adresine gönderildi.")
            else:
                await message.answer("ℹ️ PERSONAL_EMAIL tanımlı değil, rapor maili gönderilmedi.")
                
        else:
            await message.answer(f"❌ İşlem başarısız: {result.get('error', 'Bilinmeyen hata')}")
        
    except Exception as e:
        logger.error(f"PEX işleme hatası: {e}")
        await message.answer("❌ PEX işleme sırasında hata oluştu.")
    finally:
        await _cleanup_pex_files(pex_files)
        await state.clear()
        
      

# İptal komutları ve butonları
@router.message(PexProcessingStates.waiting_for_files, F.text.in_(["/dur", "/stop", "/cancel", "/iptal"]))
async def handle_pex_cancel_commands(message: Message, state: FSMContext):
    """PEX modunda iptal komutları"""
    from handlers.reply_handler import cancel_all_operations
    await cancel_all_operations(message, state)

@router.message(PexProcessingStates.waiting_for_files, F.text == "🛑 DUR")
async def handle_pex_cancel_button(message: Message, state: FSMContext):
    """PEX modunda DUR butonu"""
    from handlers.reply_handler import cancel_all_operations
    await cancel_all_operations(message, state)

@router.message(PexProcessingStates.waiting_for_files)
async def handle_wrong_pex_input(message: Message):
    """Yanlış PEX girişi - sadece dosya bekliyoruz"""
    await message.answer(
        "❌ Lütfen PDF veya Excel dosyası gönderin.\n\n"
        "📤 **DOSYA BEKLİYORUM...**\n"
        "Desteklenen formatlar: PDF, Excel (.xls, .xlsx)\n\n"
        "İşlemi başlatmak için '/tamam' yazın.\n"
        "🛑 İptal etmek için '/iptal' komutunu kullanın veya DUR butonuna basın."
    )

async def _process_pex_distribution(pex_files: List[Dict]) -> Dict:
    """PEX dosyalarını gruplara dağıtır - TEK MAIL ÇOKLU DOSYA"""
    try:
        email_results = []
        groups_processed = set()
        
        # Her şehir için grupları bul ve dosyaları gönder
        for city_name in {f['city_name'] for f in pex_files}:
            normalized_city = group_manager.normalize_city_name(city_name)
            group_ids = await group_manager.get_groups_for_city(normalized_city)
            
            # Bu şehre ait tüm dosyaları bul
            city_files = [f for f in pex_files if f['city_name'] == city_name]
            
            for group_id in group_ids:
                group_info = await group_manager.get_group_info(group_id)
                recipients = group_info.get("email_recipients", [])
                
                if recipients:
                    # Gruba bu şehrin tüm dosyalarını TEK MAIL olarak gönder
                    success = await _send_group_files_single_mail(city_files, group_info, recipients)
                    
                    groups_processed.add(group_id)
                    
                    # Sonuçları kaydet
                    for recipient in recipients:
                        email_results.append({
                            "success": success,
                            "group_id": group_id,
                            "recipient": recipient,
                            "file_count": len(city_files),
                            "city": city_name
                        })
        
        return {
            "success": True,
            "email_results": email_results,
            "groups_processed": list(groups_processed)
        }
        
    except Exception as e:
        logger.error(f"PEX dağıtım hatası: {e}")
        return {"success": False, "error": str(e)}

async def _send_group_files_single_mail(file_list: List[Dict], group_info: Dict, recipients: List[str]) -> bool:
    """Gruba tüm dosyaları TEK MAIL olarak gönderir"""
    try:
        if not file_list:
            return False
            
        group_name = group_info.get("group_name", "Grup")
        file_paths = [f['path'] for f in file_list if f['path'].exists()]
        
        if not file_paths:
            logger.warning("❌ Gönderilecek dosya bulunamadı")
            return False
        
        # Mail içeriği hazırla
        subject, body = _prepare_group_email_content(file_list, group_info)
        
        # Çoklu dosya ile TEK mail gönder
        success = await send_email_with_multiple_attachments(
            recipients, subject, body, file_paths
        )
        
        logger.info(f"{'✅' if success else '❌'} {group_name} → {len(file_list)} dosya")
        return success
        
    except Exception as e:
        logger.error(f"❌ Grup mail hatası: {e}")
        return False

async def _send_all_files_to_input_email(pex_files: List[Dict]) -> bool:
    """Tüm dosyaları INPUT_EMAIL'e TEK MAIL olarak gönderir"""
    try:
        file_paths = [f['path'] for f in pex_files if f['path'].exists()]
        
        if not file_paths:
            logger.warning("❌ Input için dosya bulunamadı")
            return False
        
        subject = f"📥 Telefon data şehir bazlı Input - {len(pex_files)} Dosya"
        body = (
            f"Merhaba,\n\n"
            f"(PEX) Telefon data işlemi için {len(pex_files)} adet dosya ektedir.\n"
            f"Dosyalar: {', '.join([f['filename'] for f in pex_files])}\n"
            f"Toplam boyut: {sum(f['path'].stat().st_size for f in pex_files) / 1024:.1f} KB\n\n"
            f"İyi çalışmalar,\nData_listesi_Hıdır"
        )
        
        success = await send_email_with_multiple_attachments(
            [config.email.INPUT_EMAIL], subject, body, file_paths
        )
        
        logger.info(f"{'✅' if success else '❌'} Input mail → {len(pex_files)} dosya")
        return success
        
    except Exception as e:
        logger.error(f"❌ Input mail hatası: {e}")
        return False

def _prepare_group_email_content(file_list: List[Dict], group_info: Dict) -> tuple:
    """Grup için email içeriğini hazırlar"""
    file_types = {f['extension'] for f in file_list}
    cities = {f['city_name'].upper() for f in file_list}
    group_name = group_info.get("group_name", group_info.get("group_id", "Grup"))
    
    subject = f"📎 {group_name} - {len(file_list)} Dosya"
    body = (
        f"Merhaba,\n\n"
        f"{group_name} grubu için {len(file_list)} adet dosya ektedir.\n"
        f"Dosya türleri: {', '.join(file_types)}\n"
        f"İlgili şehirler: {', '.join(cities)}\n"
        f"Dosyalar: {', '.join([f['filename'] for f in file_list])}\n\n"
        f"İyi çalışmalar,\nData_listesi_Hıdır"
    )
    
    return subject, body


# PEX işleme raporu oluşturur
async def _generate_pex_report(result: Dict, input_email_sent: bool, file_count: int) -> str:
    """PEX işleme raporu oluşturur"""
    if not result.get("success", False):
        return f"❌ PEX işleme başarısız: {result.get('error', 'Bilinmeyen hata')}"
    
    email_results = result.get("email_results", [])
    groups_processed = len(result.get("groups_processed", []))
    
    successful_emails = sum(1 for res in email_results if res.get("success", False))
    failed_emails = len(email_results) - successful_emails
    
    report_lines = [
        "✅ **Pdf Excel Dagıtım Raporu**",
        f"📁 İşlenen dosya: {file_count}",
        f"👥 İşlem yapılan grup: {groups_processed}",
        f"📧 Başarılı mail: {successful_emails}",
        f"❌ Başarısız mail: {failed_emails}",
        f"📥 Input mail: {'✅ Gönderildi' if input_email_sent else '❌ Gönderilmedi'}"
    ]
    
    # Grup bazlı özet
    if groups_processed > 0:
        report_lines.append("")
        report_lines.append("📋 **GRUP ÖZETİ:**")
        
        # Grupları şehirlere göre grupla
        group_cities = {}
        for res in email_results:
            if res.get("success"):
                group_id = res["group_id"]
                city = res.get("city", "")
                if group_id not in group_cities:
                    group_cities[group_id] = set()
                group_cities[group_id].add(city)
        
        for group_id, cities in group_cities.items():
            group_info = await group_manager.get_group_info(group_id)
            group_name = group_info.get("group_name", group_id)
            report_lines.append(f"• {group_name}: {', '.join([c.upper() for c in cities])}")
    
    return "\n".join(report_lines)

async def _cleanup_pex_files(pex_files: List[Dict]):
    """Geçici PEX dosyalarını temizler"""
    for file_info in pex_files:
        try:
            file_info['path'].unlink(missing_ok=True)
        except Exception:
            pass