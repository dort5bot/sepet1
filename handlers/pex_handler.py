
# PEX Handler
import asyncio

from pathlib import Path
from typing import Dict, List
from aiogram import Router, F
from aiogram.types import Message
from aiogram.filters import Command
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from datetime import datetime

from utils.mailer import send_email

from config import config
from utils.group_manager import group_manager

from utils.logger import logger

# Handler loader uyumlu router tanımı
router = Router(name="pex_processor")

class PexProcessingStates(StatesGroup):
    """PEX işleme state'leri"""
    waiting_for_files = State()
 

# PEX dosyalarını GRUP bazlı seri dağıtır
# Tek mail, çoklu dosya gönderir
# TEK MERKEZİ PEX MAIL GÖNDERİCİ
 
async def send_pex_mail(pex_files: List[Dict]) -> Dict:
    """
    PEX dosyalarını GRUP bazlı SERİ (SEQUENTIAL) mail olarak gönderir.
    - Her grup için: tek mail + çok dosya
    - Paralellik yok
    - Gmail uyumlu: her mail arası delay eklenir
    """
    try:
        order_map = []
        groups_processed = set()
        group_map = {}  # group_id -> list[files]

        # -----------------------------
        # 1) ŞEHİR → GRUP TOPLAMA
        # -----------------------------
        for f in pex_files:
            normalized_city = group_manager.normalize_city_name(f["city_name"])
            group_ids = await group_manager.get_groups_for_city(normalized_city)

            for gid in group_ids:
                group_map.setdefault(gid, []).append(f)

        # -----------------------------
        # 2) SERİ GÖNDERİM BAŞLA
        # -----------------------------
        email_results = []
        idx = 0

        for group_id, files_for_group in group_map.items():
            group_info = await group_manager.get_group_info(group_id)
            recipients = group_info.get("email_recipients", [])

            if not recipients:
                continue

            order_map.append({
                "order": idx,
                "group_id": group_id,
                "files": files_for_group,
                "group_info": group_info
            })

            # -----------------------------
            # 3) SERİ MAIL GÖNDERİMİ
            # -----------------------------
            try:
                await _send_group_mail(
                    files_for_group,
                    group_info,
                    recipients
                )
                success = True
            except Exception as e:
                logger.error(f"Mail gönderimi başarısız ({group_id}): {e}")
                success = False

            # Rapor kaydı
            for r in recipients:
                email_results.append({
                    "order": idx,
                    "success": success,
                    "group_id": group_id,
                    "recipient": r,
                    "files": [
                        {"filename": f["filename"], "city": f["city_name"]}
                        for f in files_for_group
                    ]
                })

            groups_processed.add(group_id)

            # Gmail Anti-Spam için güvenli gecikme 1.2 saniye
            # ------------------------------------
            await asyncio.sleep(1.2)

            idx += 1

        # -----------------------------
        # 4) RAPOR (Hiç mail yoksa)
        # -----------------------------
        if not email_results:
            email_results.append({
                "order": -1,
                "success": False,
                "group_id": None,
                "recipient": None,
                "filename": None,
                "city": None,
                "note": "Hiçbir gruba mail gönderilmedi."
            })

        return {
            "success": True,
            "email_results": email_results,
            "groups_processed": list(groups_processed)
        }

    except Exception as e:
        logger.error(f"PEX seri mail işlem hatası: {e}")
        return {"success": False, "error": str(e)}


# PEX dosyalarını gruplara SERİ dağıtır - TEK MAIL ÇOKLU DOSYA
# Her grup için tek mail
# Tek mail, çoklu dosya gönderir
async def _send_group_mail(
    file_list: List[Dict], 
    group_info: Dict, 
    recipients: List[str]
) -> bool:
    """Gruba tüm dosyaları TEK MAIL olarak gönderir (yeni merkezi sistem)"""
    try:
        if not file_list:
            return False

        # Dosya yollarını hazırla
        file_paths = []
        for f in file_list:
            p = Path(f["path"])
            if p.exists():
                file_paths.append(p)

        if not file_paths:
            logger.warning(f"❌ {group_info.get('group_name')}: Gönderilecek dosya bulunamadı")
            return False

        # Mail içeriğini hazırla
        subject, body = _prepare_group_email_content(file_list, group_info)

        # Her alıcıya ayrı ayrı gönder
        success = True
        for recipient in recipients:          
            ok = await send_email(
                to_emails=[recipient],
                subject=subject,
                body=body,
                attachments=file_paths
            )  
            
            if not ok:
                success = False

        logger.info(f"{'✅' if success else '❌'} {group_info.get('group_name')} → {len(file_paths)} dosya gönderildi")
        return success

    except Exception as e:
        logger.error(f"❌ Grup mail hatası ({group_info.get('group_name')}): {e}")
        return False

def _prepare_group_email_content(file_list: List[Dict], group_info: Dict) -> tuple:
    """
    Grup için email içeriğini hazırlar
    """
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


async def _send_input_email(pex_files: List[Dict]) -> bool:
    """Tüm dosyaları INPUT_EMAIL'e TEK MAIL olarak gönderir (yeni sistem)"""
    try:
        file_paths = [Path(f['path']) for f in pex_files if Path(f['path']).exists()]
        
        if not file_paths:
            logger.warning("❌ Input için dosya bulunamadı")
            return False
        
        subject = f"📥 Telpex Input pdf excel - {len(pex_files)} Dosya"
        body = (
            f"Merhaba,\n\n"
            f"Telefon data işlemi için {len(pex_files)} adet dosya ektedir.\n"
            f"Dosyalar: {', '.join([f['filename'] for f in pex_files])}\n"
            f"İyi çalışmalar,\nData_listesi_Hıdır"
        )
          
        success = await send_email(
            to_emails=[config.email.INPUT_EMAIL],
            subject=subject,
            body=body,
            attachments=file_paths,
            # email_config gerekirse burada belirtilir
        )
        
        logger.info(f"{'✅' if success else '❌'} Input mail → {len(pex_files)} dosya")
        return success
        
    except Exception as e:
        logger.error(f"❌ Input mail hatası: {e}")
        return False


# Gerçek mail adedi = email_results içinde her alıcı için oluşturulan satırlar
# Başarılı mail = success=True olan satırlar
# Başarısız mail = success=False olan satırlar
# Input mail → ayrıca 1 adet işlem olarak eklenir

async def _send_personal_email(result: Dict, input_email_sent: bool, file_count: int) -> str:
    """PEX işleme raporu oluşturur (DOĞRU MAIL SAYIMI İLE)"""

    # ---- 1) Genel hata kontrolü ----
    if not result.get("success", False):
        return f"❌ PEX işleme başarısız: {result.get('error', 'Bilinmeyen hata')}"

    email_results = result.get("email_results", [])
    groups_processed = len(result.get("groups_processed", []))

    # ---- 2) MAIL SAYIMI (DOĞRU YÖNTEM) ----
    successful_emails = sum(1 for r in email_results if r.get("success"))
    failed_emails = sum(1 for r in email_results if not r.get("success"))

    # Input maili ekle
    if input_email_sent:
        successful_emails += 1
    else:
        failed_emails += 1

    # ---- 3) Grup bazlı başarı/başarısızlık ----
    group_status: Dict[str, bool] = {}
    for res in email_results:
        gid = res.get("group_id")
        if gid is None:
            continue
        prev = group_status.get(gid, False)
        group_status[gid] = prev or bool(res.get("success"))

    # ---- 4) Grup -> şehir eşlemesi ----
    group_cities: Dict[str, set] = {}
    for res in email_results:
        gid = res.get("group_id")
        if gid is None:
            continue

        # Yeni format: files[]
        files = res.get("files", [])
        for f in files:
            city = (f.get("city") or "").upper().strip()
            if city:
                group_cities.setdefault(gid, set()).add(city)

    # ---- 5) Rapor metni ----
    report_lines = [
        "✅ **Pdf Excel Dağıtım Raporu**\n",
        f"⏰ İşlem zamanı: {datetime.now().strftime('%d.%m.%Y %H:%M')}",
        "",
        f"📂 Eklenen(İnput) dosya: {file_count}",
        f"👥 Oluşan grup dosyası: {groups_processed}",
        f"📧 Başarılı mail: {successful_emails}",
        f"❌ Başarısız mail: {failed_emails}",
        f"📥 Input mail: {'✅ Gönderildi' if input_email_sent else '❌ Gönderilmedi'}",
    ]

    # ---- 6) Grup şehir listesi ----
    if groups_processed > 0 and group_cities:
        report_lines.append("")
        report_lines.append(f"📋 *Grup Dosyaları ({groups_processed})*")

        for gid, cities in group_cities.items():
            group_info = await group_manager.get_group_info(gid)
            group_name = group_info.get("group_name", gid)
            cities_str = ", ".join(sorted(cities)) if cities else "—"
            report_lines.append(f"• {group_name}: {cities_str}")

    return "\n".join(report_lines)




# ================== komut blok ==============================

# PEX işlemini başlat - (RAPOR MAILI EKLENDİ)
# Input mail → Grup mailleri  → Personal mail

@router.message(Command("pex"))
async def cmd_pex(message: Message, state: FSMContext):
    """PEX - Dosya adı bazlı dağıtım komutu"""
    await state.set_state(PexProcessingStates.waiting_for_files)
    await message.answer(
        "📁 **PEX MODU - DOSYA ADI BAZLI DAĞITIM**\n\n"
        # "Lütfen dağıtmak istediğiniz dosyaları gönderin.\n\n"
        "📋 **KURALLAR:**\n"
        "• Dosya adı SADECE  şehir adı olmalı: ankara gibi\n"
        "• Desteklenenler: PDF, Excel, Word, resim, arşiv\n\n"
        
        "• ilk dosyayı TEK gönder(zorunlu)\n"
        "• sonrakiler TOPLU gönderilebilir\n\n"
        
        "🔄 **İŞLEM:**\n"
        "1. Dosya adındaki şehir gruplarda aranır\n"
        "2. Eşleşen tüm gruplara dosya gönderilir\n"
        "3. Her grup kendi email listesine ulaşır\n\n"
        
        "📤 **DOSYA BEKLİYORUM...**\n"
        "Lütfen dosya gönderin.\n\n"
        "🛑 İptal için '/iptal' komutu kullan veya DUR a bas."
    )


# 1️ İptal komutları
# @router.message(PexProcessingStates.waiting_for_files, F.text.in_(["/dur", "/stop", "/cancel", "/iptal"]))
@router.message(
    PexProcessingStates.waiting_for_files,
    Command(commands=["dur", "stop", "cancel", "iptal"])
)
async def handle_pex_cancel_commands(message: Message, state: FSMContext):
    """PEX modunda iptal komutları"""
    from handlers.reply_handler import cancel_all_operations
    await cancel_all_operations(message, state)


# 2️ DUR butonu
@router.message(PexProcessingStates.waiting_for_files, F.text == "🛑 DUR")
async def handle_pex_cancel_button(message: Message, state: FSMContext):
    """PEX modunda DUR butonu"""
    from handlers.reply_handler import cancel_all_operations
    await cancel_all_operations(message, state)


# 3️ /tamam
# @router.message(PexProcessingStates.waiting_for_files, F.text == "/tamam")
@router.message( PexProcessingStates.waiting_for_files,Command("tamam"))

async def handle_process_pex(message: Message, state: FSMContext):
    """PEX işlemini başlat (Aşama 1 + 2 seri, rapor bağımlı)"""
    data = await state.get_data()
    pex_files = data.get("pex_files", [])

    if not pex_files:
        await message.answer("❌ İşlenecek dosya yok.")
        await state.clear()
        return

    await message.answer(
        "⏳ PEX dağıtım işlemi başlıyor...\n"
        "📨 Gmail uyumu: mailler *seri* gönderilir..."
    )

    try:
        # -------------------------------
        # AŞAMA 1 → INPUT MAIL (Seri)
        # -------------------------------
        input_email_sent = await _send_input_email(pex_files)

        # -------------------------------
        # AŞAMA 2 → GRUP MAILLERİ (Seri)
        # -------------------------------
        group_result = await send_pex_mail(pex_files)

        # -------------------------------
        # AŞAMA 3 → RAPOR
        # -------------------------------
        report = await _send_personal_email(group_result, input_email_sent, len(pex_files))
        await message.answer(report)

        # Kişisel e-posta gönderimi
        if config.email.PERSONAL_EMAIL:
            await send_email(
                to_emails=[config.email.PERSONAL_EMAIL],
                subject=f"📊 PEX Raporu - {len(pex_files)} Dosya",
                # html_body=None,  # veya HTML versiyonu
                # attachments=None,  # rapor ekli değil
                body=report
            )

    except Exception as e:
        logger.error(f"PEX işleme hatası: {e}")
        await message.answer("❌ PEX işleme sırasında hata oluştu.")

    finally:
        await _cleanup_pex_files(pex_files)
        await state.clear()


# 4️ BELGE: belge → belge handler, hatalı belge yakalar
@router.message(PexProcessingStates.waiting_for_files, F.document)
async def handle_pex_file_upload(message: Message, state: FSMContext):
    """PEX dosyalarını işler"""
    # Dosya formatı kontrolü
    valid_extensions = {
        # Mevcut formatlar
        '.pdf', '.xls', '.xlsx',
        # Yeni eklenen formatlar
        '.csv', '.doc', '.docx', '.txt', '.rtf',
        '.ppt', '.pptx', '.odt', '.ods', '.odp',
        '.jpg', '.jpeg', '.png', '.gif', '.bmp',
        '.zip', '.rar', '.7z'
    }
    
    
    file_ext = Path(message.document.file_name).suffix.lower()
    
    if file_ext not in valid_extensions:
        await message.answer("❌ Desteklenmeyen dosya formatı. -yalnız: pdf, doc, docx, excel, csv, zip, jpg, jpeg, png, ...")
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
            "Dosya varsa ekle, dağıtmak için '/tamam' tıkla yada yaz\n\n"
            "🛑 İptal için '/iptal' veya DUR butonu"
        )
        
    except Exception as e:
        logger.error(f"PEX dosya işleme hatası: {e}")
        await message.answer("❌ Dosya işlenirken hata oluştu.")


# 5️ ❗ EN SON: catch-all: hata yakalama
@router.message(PexProcessingStates.waiting_for_files)
async def handle_wrong_pex_input(message: Message):
    """Yanlış PEX girişi - sadece dosya bekliyoruz"""
    await message.answer(
        "❌ Lütfen PDF, Excel vb dosyası gönderin.\n\n"
        "📤 **DOSYA BEKLİYORUM...**\n"
        "Desteklenenler: PDF, Excel, word, resim, ...\n\n"
        "İşlemi başlatmak için '/tamam' tıkla yada yazın.\n"
        "🛑 İptal etmek için '/iptal' yazın veya DUR butonuna basın."
    )


async def _cleanup_pex_files(pex_files: List[Dict]) -> None:
    """
    Geçici PEX dosyalarını güvenli şekilde temizler.
    Python 3.11+ uyumludur.
    """
    for file_info in pex_files:
        path = file_info.get("path")

        # Path kontrolü (defansif programlama)
        if not isinstance(path, Path):
            continue

        try:
            path.unlink(missing_ok=True)
        except PermissionError as e:
            # Dosya kilitliyse (özellikle Windows)
            logger.warning(f"⚠️ Dosya silinemedi (kilitli): {path} - {e}")
        except Exception as e:
            # Diğer beklenmeyen hatalar
            logger.error(f"❌ Dosya silme hatası: {path} - {e}")

