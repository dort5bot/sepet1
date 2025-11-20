# handlers/pex_handler.py
"""
PEX Handler Module
Dosya adı bazlı dağıtım işlemleri

17-11-2025
"""
import tempfile
import zipfile
from pathlib import Path
from typing import Dict, List, Set
from aiogram import Router, F
from aiogram.types import Message
from aiogram.filters import Command
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup

from config import config
from utils.group_manager import group_manager
from utils.mailer import send_email_with_attachment
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
        "• Aynı anda birden fazla dosya gönderebilirsiniz\n\n"
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
        
        # Dosyayı indir - DÜZELTİLDİ: config.paths.INPUT_DIR
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
            "📤 **DOSYA BEKLİYORUM...**\n"
            "Başka dosya gönderebilir veya işlemi başlatmak için '/tamam' yazın.\n\n"
            "🛑 İptal için '/iptal' veya DUR butonu"
        )
        
    except Exception as e:
        logger.error(f"PEX dosya işleme hatası: {e}")
        await message.answer("❌ Dosya işlenirken hata oluştu.")
        

@router.message(PexProcessingStates.waiting_for_files, F.text == "/tamam")
async def handle_process_pex(message: Message, state: FSMContext):
    """PEX işlemini başlat"""
    data = await state.get_data()
    pex_files = data.get('pex_files', [])
    
    if not pex_files:
        await message.answer("❌ İşlenecek dosya yok.")
        await state.clear()
        return
    
    await message.answer("⏳ Dosyalar gruplara dağıtılıyor ve mailler hazırlanıyor...")
    
    try:
        # Dosyaları gruplara göre işle
        result = await _process_pex_distribution(pex_files)
        
        if result["success"]:
            report = await _generate_pex_report(result)  # ✅ await eklendi
            await message.answer(report)
        else:
            await message.answer(f"❌ İşlem başarısız: {result.get('error', 'Bilinmeyen hata')}")
        
    except Exception as e:
        await message.answer("❌ PEX işleme sırasında hata oluştu.")
    finally:
        await _cleanup_pex_files(pex_files)
        await state.clear()



# 🆕 PEX STATE'İNDE TÜM İPTAL KOMUTLARI VE BUTONLARI
@router.message(PexProcessingStates.waiting_for_files, F.text.in_(["/dur", "/stop", "/cancel", "/iptal"]))
async def handle_pex_cancel_commands(message: Message, state: FSMContext):
    """PEX modunda iptal komutları"""
    from handlers.reply_handler import cancel_all_operations
    await cancel_all_operations(message, state)

# 🆕 BUTON MESAJLARI İÇİN AYRI HANDLER
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
    """PEX dosyalarını gruplara dağıtır"""
    try:
        # 1. Dosyaları şehirlere göre gruplandır
        city_to_files = _group_files_by_city(pex_files)
        
        # 2. Her şehir için ilgili grupları bul - DÜZELTİLDİ
        group_to_files = await _map_groups_to_files(city_to_files)
        
        if not group_to_files:
            return {"success": False, "error": "Hiçbir grup bulunamadı"}
        
        # 3. Her grup için dosyaları birleştir ve mail gönder
        email_results = await _process_group_distributions(group_to_files)
        
        return {
            "success": True,
            "processed_files": len(pex_files),
            "groups_processed": len(group_to_files),
            "email_results": email_results,
            "group_details": group_to_files
        }
        
    except Exception as e:
        logger.error(f"PEX dağıtım hatası: {e}")
        return {"success": False, "error": str(e)}

def _group_files_by_city(pex_files: List[Dict]) -> Dict[str, List[Dict]]:
    """Dosyaları şehir adlarına göre gruplandırır"""
    city_to_files = {}
    for file_info in pex_files:
        city_name = file_info['city_name']
        if city_name not in city_to_files:
            city_to_files[city_name] = []
        city_to_files[city_name].append(file_info)
    return city_to_files


async def _map_groups_to_files(city_to_files: Dict[str, List[Dict]]) -> Dict[str, List[Dict]]:
    """Şehir-grup eşleştirmesi yapar"""
    group_to_files = {}
    
    for city_name, file_list in city_to_files.items():
        normalized_city = group_manager.normalize_city_name(city_name)
        # ✅ DOĞRU: Async metot await ile çağrılıyor
        group_ids = await group_manager.get_groups_for_city(normalized_city)
        
        for group_id in group_ids:
            if group_id not in group_to_files:
                group_to_files[group_id] = []
            group_to_files[group_id].extend(file_list)
    
    return group_to_files


async def _process_group_distributions(group_to_files: Dict[str, List[Dict]]) -> List[Dict]:
    """Gruplara dosya dağıtımını işler"""
    email_results = []
    
    for group_id, file_list in group_to_files.items():
        if not file_list:
            continue
            
        # ✅ DOĞRU: Async metot await ile çağrılıyor
        group_info = await group_manager.get_group_info(group_id)
        recipients = group_info.get("email_recipients", [])
        
        if not recipients:
            continue
        
        # Dosyaları ZIP yap ve mail gönder
        result = await _send_group_files(file_list, group_info, recipients)
        if result:
            email_results.extend(result)
    
    return email_results
    

# Grup dosyalarını ZIP yaparak mail gönderir
r""" async def _send_group_files(file_list: List[Dict], group_info: Dict, recipients: List[str]) -> List[Dict]:
    try:
        zip_path = await _create_pex_zip(file_list, group_info)
        if not zip_path:
            return []
        
        # Mail içeriği hazırla
        subject, body = _prepare_email_content(file_list, group_info)
        
        # Mail gönder
        success = await send_email_with_attachment(
            recipients, subject, body, zip_path
        )
        
        # Sonuçları hazırla
        results = []
        for recipient in recipients:
            results.append({
                "success": success,
                "group_id": group_info.get("group_id"),
                "recipient": recipient,
                "file_count": len(file_list),
                "cities": list({f['city_name'] for f in file_list})
            })
        
        # Geçici ZIP'i sil
        zip_path.unlink(missing_ok=True)
        return results
        
    except Exception:
        return []
"""

async def _send_group_files(file_list: List[Dict], group_info: Dict, recipients: List[str]) -> List[Dict]:
    """Grup dosyalarını ZIP yaparak mail gönderir"""
    try:
        zip_path = await _create_pex_zip(file_list, group_info)
        if not zip_path:
            logger.error("❌ ZIP dosyası oluşturulamadı")
            return []
        
        # DEBUG: ZIP kontrolü
        logger.info(f"🔍 DEBUG - ZIP oluşturuldu: {zip_path}, exists: {zip_path.exists()}")
        
        # Mail içeriği hazırla
        subject, body = _prepare_email_content(file_list, group_info)
        
        # DEBUG: Mail bilgileri
        #logger.info(f"🔍 DEBUG - Mail hazırlanıyor:")
        #logger.info(f"🔍 DEBUG - Alıcılar: {recipients}")
        #logger.info(f"🔍 DEBUG - Konu: {subject}")
        #logger.info(f"🔍 DEBUG - Grup: {group_info.get('group_name')}")
        
        # Mail gönder
        success = await send_email_with_attachment(
            recipients, subject, body, zip_path
        )
        
        # DEBUG: Mail sonucu
        logger.info(f"🔍 DEBUG - Mail gönderim sonucu: {success}")
        
        # Sonuçları hazırla
        results = []
        for recipient in recipients:
            results.append({
                "success": success,
                "group_id": group_info.get("group_id"),
                "recipient": recipient,
                "file_count": len(file_list),
                "cities": list({f['city_name'] for f in file_list})
            })
        
        # Geçici ZIP'i sil
        zip_path.unlink(missing_ok=True)
        return results
        
    except Exception as e:
        logger.error(f"❌ _send_group_files hatası: {e}")
        return []

async def _create_pex_zip(file_list: List[Dict], group_info: Dict) -> Path:
    """Dosyaları ZIP olarak paketler"""
    try:
        group_name = group_info.get("group_name", "dosyalar")
        zip_name = f"{group_name}_dosyalar.zip"
        zip_path = Path(tempfile.gettempdir()) / zip_name
        
        with zipfile.ZipFile(zip_path, 'w', zipfile.ZIP_DEFLATED) as zipf:
            for file_info in file_list:
                if file_info['path'].exists():
                    zipf.write(file_info['path'], file_info['filename'])
        
        return zip_path
    except Exception:
        return None

def _prepare_email_content(file_list: List[Dict], group_info: Dict) -> tuple:
    """Email içeriğini hazırlar"""
    file_types = {f['extension'] for f in file_list}
    cities = {f['city_name'].upper() for f in file_list}
    group_name = group_info.get("group_name", group_info.get("group_id", "Grup"))
    
    subject = f"📎 {group_name} - {len(file_list)} Dosya"
    body = (
        f"Merhaba,\n\n"
        f"{group_name} grubu için {len(file_list)} adet dosya ektedir.\n"
        f"Dosya türleri: {', '.join(file_types)}\n"
        f"İlgili şehirler: {', '.join(cities)}\n\n"
        f"İyi çalışmalar,\nData_listesi_Hıdır"
    )
    
    return subject, body


async def _generate_pex_report(result: Dict) -> str:  # ✅ async eklenmeli
    """PEX işleme raporu oluşturur - DÜZELTİLDİ"""
    if not result.get("success", False):
        return f"❌ PEX işleme başarısız: {result.get('error', 'Bilinmeyen hata')}"
    
    processed_files = result.get("processed_files", 0)
    groups_processed = result.get("groups_processed", 0)
    email_results = result.get("email_results", [])
    
    successful_emails = sum(1 for res in email_results if res.get("success", False))
    failed_emails = len(email_results) - successful_emails
    
    report_lines = [
        "✅ **PEX DAĞITIM RAPORU**",
        f"📁 İşlenen dosya: {processed_files}",
        f"👥 İşlem yapılan grup: {groups_processed}",
        f"📧 Başarılı mail: {successful_emails}",
        f"❌ Başarısız mail: {failed_emails}",
        "",
        "📋 **GRUP DETAYLARI:**"
    ]
    
    # Grup bazlı detaylar - ✅ DÜZELTİLDİ
    group_details = result.get("group_details", {})
    for group_id, file_list in group_details.items():
        group_info = await group_manager.get_group_info(group_id)  # ✅ await eklendi
        group_name = group_info.get("group_name", group_id)
        cities = {f['city_name'].upper() for f in file_list}
        report_lines.append(f"• {group_name}: {len(file_list)} dosya ({', '.join(cities)})")
    
    return "\n".join(report_lines)
    

async def _cleanup_pex_files(pex_files: List[Dict]):
    """Geçici PEX dosyalarını temizler"""
    for file_info in pex_files:
        try:
            file_info['path'].unlink(missing_ok=True)
        except Exception:
            pass