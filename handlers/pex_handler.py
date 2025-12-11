# handlers/pex_handler.py
"""
PEX Handler Module - GÜNCELLENMİŞ VERSİYON
Dosya adı bazlı dağıtım işlemleri (ZIP'siz doğrudan gönderim)

version: 27-11-2025

PEX Mail İşlem Akışı — Senin anlattığın
AŞAMA 1 — INPUT için Mail (paralel yapılabilir)
Input klasörüne gelen tüm dosyalar tek bir mailde gönderilir.
Bu mail tek maildir, dosya sayısı ne olursa olsun.
Bu işlem diğer gruplarla paralel yürüyebilir.
AŞAMA 2 — Gruplar için Mail (paralel yapılabilir)
Her grup için ayrı bir mail gönderilecek.
Bir dosya birden fazla grupla ilişkili olabilir → her ilişki için mail gider.
Bir grubun mailine bir ya da birden fazla dosya eklenebilir.
Ama gruba kaç dosya düşerse düşsün, tek mail gönderilecek.
Bu grup mailleri de birbirleriyle paralel yapılabilir.
AŞAMA 3 — RAPOR Maili (BAĞIMLI → 1 ve 2 bitmeden başlayamaz)
Tüm input maili ve grup mailleri bittikten sonra tek bir rapor maili gönderilir.
Paralel olamaz.
1 ve 2 tamamlanmadan başlatılırsa raporlama hatası çıkıyor (bunu da biliyorum).

"""

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

from config import config
from utils.group_manager import group_manager
from utils.mailer import MailerV2
from utils.logger import logger

# Handler loader uyumlu router tanımı
router = Router(name="pex_processor")

class PexProcessingStates(StatesGroup):
    """PEX işleme state'leri"""
    waiting_for_files = State()

# Mailer instance'ı
_pex_mailer = None

async def get_pex_mailer() -> MailerV2:
    """PEX işlemleri için mailer instance'ını döndürür"""
    global _pex_mailer
    if _pex_mailer is None:
        _pex_mailer = MailerV2()
        await _pex_mailer.start()
    return _pex_mailer

async def cleanup_pex_mailer():
    """PEX mailer'ı temizler"""
    global _pex_mailer
    if _pex_mailer is not None:
        await _pex_mailer.stop()
        _pex_mailer = None
        
@router.message(Command("pex"))
async def cmd_pex(message: Message, state: FSMContext):
    """PEX - Dosya adı bazlı dağıtım komutu"""
    await state.set_state(PexProcessingStates.waiting_for_files)
    await message.answer(
        "📁 **PEX MODU - DOSYA ADI BAZLI DAĞITIM**\n\n"
        "Lütfen dağıtmak istediğiniz dosyaları gönderin.\n\n"
        "📋 **KURALLAR:**\n"
        "• Dosya adı SADECE  şehir adı olmalı: ankara gibi\n"
        "• Desteklenen formatlar: PDF, Excel (.xls, .xlsx)\n"
        "• ilk dosyayı TEK gönder(en iyisi bu yöntem)\n"
        "• sonra TOPLU gönderilebilir\n\n"
        
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
# PEX işlemini başlat - (RAPOR MAILI EKLENDİ)
@router.message(PexProcessingStates.waiting_for_files, F.text == "/tamam")
async def handle_process_pex(message: Message, state: FSMContext):
    """PEX işlemini başlat (Aşama 1 + 2 paralel, rapor bağımlı)"""
    data = await state.get_data()
    pex_files = data.get("pex_files", [])

    if not pex_files:
        await message.answer("❌ İşlenecek dosya yok.")
        await state.clear()
        return

    await message.answer("⏳ PEX dağıtım işlemi başlıyor...\n"
                         "işlemde  1.(Input mail) + 2.(Grup mailleri) paralel çalışır...")

    try:
        # -------------------------------
        # AŞAMA 1 + AŞAMA 2 → paralel
        # -------------------------------
        task_input = asyncio.create_task(_send_all_files_to_input_email(pex_files))
        task_groups = asyncio.create_task(_process_pex_distribution_parallel(pex_files))

        input_email_sent, group_result = await asyncio.gather(task_input, task_groups)

        # -------------------------------
        # AŞAMA 3 → RAPOR oluşturma (BAĞIMLI)
        # -------------------------------
        report = await _generate_pex_report(group_result, input_email_sent, len(pex_files))
        await message.answer(report)

        # Raporu personal email’e gönder
        if config.email.PERSONAL_EMAIL:
            mailer = await get_pex_mailer()
            await mailer.send_simple_email(
                [config.email.PERSONAL_EMAIL],
                f"📊 PEX Raporu - {len(pex_files)} Dosya",
                report
            )

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

# PEX dosyalarını gruplara dağıtır - TEK MAIL ÇOKLU DOSYA
# PEX dosyalarını GRUP bazlı paralel dağıtır
# ← rapor sıralaması
# 1) GRUP BAZLI TOPLAMA
# 2) Her grup için tek mail
# Tek mail, çoklu dosya gönderir

async def _process_pex_distribution_parallel(pex_files: List[Dict]) -> Dict:
    """ bu bölüm içinde silme olmayacak, sonra mail eksik oluyor.
    tüm işlemler bittikten sonra silme işlemi zaten vars
    """
    try:
        tasks = []
        order_map = []
        groups_processed = set()

        mailer = await get_pex_mailer()
        group_map = {}  # group_id -> list[files]

        # -----------------------------
        # 1) ŞEHİR → GRUP EŞLEŞTİRME | TOPLAMA
        # -----------------------------
        for f in pex_files:
            normalized_city = group_manager.normalize_city_name(f["city_name"])
            group_ids = await group_manager.get_groups_for_city(normalized_city)

            for gid in group_ids:
                group_map.setdefault(gid, []).append(f)

        # -----------------------------
        # 2) GRUP → TEK MAIL + ÇOK DOSYA
        # -----------------------------
        for idx, (group_id, files_for_group) in enumerate(group_map.items()):
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

            tasks.append(asyncio.create_task(
                _send_group_files_single_mail(
                    files_for_group,
                    group_info,
                    recipients
                )
            ))

            groups_processed.add(group_id)

        # -----------------------------
        # 3) GÖNDERİMLERİ BEKLE
        # -----------------------------
        results = await asyncio.gather(*tasks, return_exceptions=True)


        # -----------------------------
        # 4) RAPOR FORMATLA - rapor sayısı - 
        # -----------------------------
        email_results = []
        for idx, entry in enumerate(order_map):
            success = not isinstance(results[idx], Exception)
            files = entry["files"]
            group_info = entry["group_info"]
            group_id = entry["group_id"]
            recipients = group_info.get("email_recipients", [])

            """ MAİL SAYISI SORUNU ÇÖZÜLDÜ, RAPOR YAPISI EKSİLDİ
            for r in recipients:
                email_results.append({
                    "order": idx,
                    "success": success,
                    "group_id": group_id,
                    "recipient": r,
                    
                })
            """

            for r in recipients:
                email_results.append({
                    "order": idx,
                    "success": success,
                    "group_id": group_id,
                    "recipient": r,
                    # Dosya/şehir bilgisini liste olarak ekliyoruz
                    "files": [
                        {"filename": f["filename"], "city": f["city_name"]}
                        for f in files
                    ]
                })


        # Hiç mail atılmadıysa rapora ek bilgi
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
        logger.error(f"PEX dağıtım hatası: {e}")
        return {"success": False, "error": str(e)}

# Fonksiyonları güncelle:
# Gruba tüm dosyaları TEK MAIL olarak gönderir

async def _send_group_files_single_mail(file_list: List[Dict], group_info: Dict, recipients: List[str]) -> bool:
    """Gruba tüm dosyaları TEK MAIL olarak gönderir"""
    try:
        if not file_list:
            return False

        group_name = group_info.get("group_name", "Grup")

        # Dosya yollarını güvenli şekilde hazırlıyoruz
        file_paths = []
        for f in file_list:
            p = Path(f["path"])
            if p.exists():
                file_paths.append(p)

        if not file_paths:
            logger.warning(f"❌ {group_name}: Gönderilecek dosya bulunamadı")
            return False

        # Mail içeriği
        subject, body = _prepare_group_email_content(file_list, group_info)

        # Mailer
        mailer = await get_pex_mailer()

        # Gönder
        success = True
        for r in recipients:
            ok = await mailer.send_email_with_multiple_attachments(
                [r],  # TEK ALICI
                subject,
                body,
                file_paths
            )
            if not ok:
                success = False


        logger.info(f"{'✅' if success else '❌'} {group_name} → {len(file_paths)} dosya gönderildi")
        return success

    except Exception as e:
        logger.error(f"❌ Grup mail hatası ({group_info.get('group_name')}): {e}")
        return False


async def _send_all_files_to_input_email(pex_files: List[Dict]) -> bool:
    """Tüm dosyaları INPUT_EMAIL'e TEK MAIL olarak gönderir"""
    try:
        file_paths = [f['path'] for f in pex_files if f['path'].exists()]
        
        if not file_paths:
            logger.warning("❌ Input için dosya bulunamadı")
            return False
        
        subject = f"📥 Telpex Input şehir - {len(pex_files)} Dosya"
        body = (
            f"Merhaba,\n\n"
            f"Telefon data işlemi için {len(pex_files)} adet dosya ektedir.\n"
            f"Dosyalar: {', '.join([f['filename'] for f in pex_files])}\n"
            #f"Toplam boyut: {sum(f['path'].stat().st_size for f in pex_files) / 1024:.1f} KB\n\n"
            f"İyi çalışmalar,\nData_listesi_Hıdır"
        )
        
        mailer = await get_pex_mailer()
        success = await mailer.send_email_with_multiple_attachments(
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
    """PEX işleme raporu oluşturur (sıralı email_results + düzenli grup özeti)"""
    
"""async def _generate_pex_report(result: Dict, input_email_sent: bool, file_count: int) -> str:

    # Başarısız ise direkt hata dön
    if not result.get("success", False):
        return f"❌ PEX işleme başarısız: {result.get('error', 'Bilinmeyen hata')}"
    
    # -------------------------------------------------------
    # 1) E-postaları işlem sırasına göre sırala
    # -------------------------------------------------------
    email_results = sorted(result.get("email_results", []), key=lambda x: x["order"])

    groups_processed = len(result.get("groups_processed", []))
    
    # successful_emails = sum(1 for res in email_results if res.get("success", False))
    # failed_emails = len(email_results) - successful_emails
    
    # Başarıyla gönderilmiş tüm gerçek mail satırlarının sayılması
    successful_emails = sum(
        1 for res in email_results
        if res.get("success") and res.get("recipient")
    )

    failed_emails = sum(
        1 for res in email_results
        if not res.get("success") and res.get("recipient")
    )




    # -------------------------------------------------------
    # 2) Raporun temel satırları
    # -------------------------------------------------------
    report_lines = [
        "✅ **Pdf Excel Dağıtım Raporu**",
        f"⏰ İşlem zamanı: {datetime.now().strftime('%d.%m.%Y %H:%M')}",
        "",
        f"📂 Eklenen(İnput) dosya: {file_count}",
        f"👥 İşlenen grup sayısı: {groups_processed}",
        f"📧 Başarılı mail: {successful_emails}",
        f"❌ Başarısız mail: {failed_emails}",
        f"📥 Input mail: {'✅ Gönderildi' if input_email_sent else '❌ Gönderilmedi'}"
    ]
    
    # -------------------------------------------------------
    # 3) Grup bazlı özet (tek tek şehir gösterimi)
    # -------------------------------------------------------
    if groups_processed > 0:
        report_lines.append("")
        report_lines.append("📋 *Grup Dosyaları:*")
        
        # Her grup -> şehir listesi
        group_cities = {}

        for res in email_results:
            if res.get("success"):
                group_id = res["group_id"]
                city = res.get("city", "").upper()

                if group_id not in group_cities:
                    group_cities[group_id] = set()
                
                if city:
                    group_cities[group_id].add(city)
        
        for group_id in group_cities.keys():
            group_info = await group_manager.get_group_info(group_id)
            group_name = group_info.get("group_name", group_id)

            cities_str = ", ".join(sorted(group_cities[group_id]))
            report_lines.append(f"• {group_name}: {cities_str}")
    
    return "\n".join(report_lines)
"""

async def _generate_pex_report(result: Dict, input_email_sent: bool, file_count: int) -> str:
    """PEX işleme raporu oluşturur (sıralı email_results + düzenli grup özeti)"""

    # Başarısız ise direkt hata dön
    if not result.get("success", False):
        return f"❌ PEX işleme başarısız: {result.get('error', 'Bilinmeyen hata')}"

    email_results = result.get("email_results", [])
    groups_processed = len(result.get("groups_processed", []))

    # -------------------------------------------------------
    # ---------- Grup bazlı başarı/başarısızlık hesapla ----------
    # email_results içinde birden fazla alıcı satırı olabilir; buradan grup bazlı durumu çıkarıyoruz.
    group_status: Dict = {}
    for res in email_results:
        gid = res.get("group_id")
        if gid is None:
            continue
        # Eğer herhangi bir satırda success True ise o grup başarılı kabul edilir
        prev = group_status.get(gid, False)
        group_status[gid] = prev or bool(res.get("success"))

    successful_groups = sum(1 for ok in group_status.values() if ok)
    failed_groups = sum(1 for ok in group_status.values() if not ok)

    # Input mail varsa bunu "başarılı mail" sayımına ekleyelim (ör. input gönderildi -> +1)
    successful_emails = successful_groups + (1 if input_email_sent else 0)
    failed_emails = failed_groups + (0 if input_email_sent else 0 if input_email_sent else 0)

    # -------------------------------------------------------
    # ---------- Grup bazlı şehir listesini oluştur ----------
    group_cities: Dict[str, set] = {}
    for res in email_results:
        gid = res.get("group_id")
        if gid is None:
            continue

        # Öncelik: res içinde "files" listesi varsa ondan şehirleri al
        files = res.get("files")
        if files and isinstance(files, list):
            for f in files:
                city = (f.get("city") or f.get("city_name") or "").strip().upper()
                if city:
                    group_cities.setdefault(gid, set()).add(city)
            continue

        # Eğer "files" yoksa, eski tekil alanları kontrol et
        city = (res.get("city") or res.get("city_name") or "").strip().upper()
        if city:
            group_cities.setdefault(gid, set()).add(city)

    # -------------------------------------------------------
    # ---------- Rapor satırlarını hazırla ----------
    report_lines = [
        "✅ **Pdf Excel Dağıtım Raporu**",
        f"⏰ İşlem zamanı: {datetime.now().strftime('%d.%m.%Y %H:%M')}",
        "",
        f"📂 Eklenen(İnput) dosya: {file_count}",
        f"👥 İşlenen grup sayısı: {groups_processed}",
        f"📧 Başarılı mail: {successful_emails}",
        f"❌ Başarısız mail: {failed_emails}",
        f"📥 Input mail: {'✅ Gönderildi' if input_email_sent else '❌ Gönderilmedi'}"
    ]

    # -------------------------------------------------------
    # Grup bazlı özet (grup adı ve şehirler)
    if groups_processed > 0 and group_cities:
        report_lines.append("")
        report_lines.append("📋 *Grup Dosyaları:*")

        for gid, cities in group_cities.items():
            # group_name almak için group_manager kullan
            group_info = await group_manager.get_group_info(gid)
            group_name = group_info.get("group_name", gid)
            cities_str = ", ".join(sorted(cities)) if cities else "—"
            report_lines.append(f"• {group_name}: {cities_str}")

    return "\n".join(report_lines)


async def _cleanup_pex_files(pex_files: List[Dict]):
    """Geçici PEX dosyalarını temizler"""
    for file_info in pex_files:
        try:
            file_info['path'].unlink(missing_ok=True)
        except Exception:
            pass
    
    # Mailer'ı da temizle
    await cleanup_pex_mailer()