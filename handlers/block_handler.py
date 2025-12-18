"""
Blok bazlı Excel işleme handler'ı
AKIŞ:
1) Ana Excel alınır
2) Veri Excel alınır
3) TC MERGE yapılır
4) MERGED dosya bloklamaya girer
5) Çıktılar mail + raporlanır
"""

import asyncio
from typing import Dict, List
from pathlib import Path
import tempfile

from aiogram import Router, F
from aiogram.types import Message
from aiogram.filters import Command
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup

from config import config
from utils.tc_merger import build_merged_excel
from utils.excel_cleaner import AsyncExcelCleaner
from utils.block_splitter import split_excel_by_blocks
from utils.reporter import generate_processing_report
from utils.mailer import send_email
from utils.group_manager import group_manager
from utils.logger import logger


router = Router(name="block_processor")


# block_handler.py'nin en üstüne ekle (import'lardan sonra):
# mail sayım işlemleri

def build_mail_result(mail_type: str, success: bool, recipient=None, filename=None, **extra) -> Dict:
    return {
        "mail_type": mail_type,
        "success": success,
        "recipient": recipient,
        "filename": filename,
        **extra
    }

def calculate_mail_stats(mail_results: List[Dict]) -> Dict:
    return {
        "total": sum(1 for m in mail_results if m["mail_type"] in ("group", "input")),
        "sent": sum(1 for m in mail_results if m["mail_type"] in ("group", "input") and m["success"]),
        "failed": sum(1 for m in mail_results if m["mail_type"] in ("group", "input") and not m["success"]),
        "by_type": {
            "group": sum(1 for m in mail_results if m["mail_type"] == "group"),
            "group_sent": sum(1 for m in mail_results if m["mail_type"] == "group" and m["success"]),
            "input": sum(1 for m in mail_results if m["mail_type"] == "input"),
            "input_sent": sum(1 for m in mail_results if m["mail_type"] == "input" and m["success"]),
            "personal": sum(1 for m in mail_results if m["mail_type"] == "personal"),
            "personal_sent": sum(1 for m in mail_results if m["mail_type"] == "personal" and m["success"]),
        }
    }
    
    
   



# ===================== FSM =====================

class BlockProcessingStates(StatesGroup):
    waiting_for_main = State()   # Ana dosya
    waiting_for_data = State()   # Veri dosyası


# ===================== HELPERS =====================

async def _download_excel(message: Message) -> Path:
    """Telegram'dan Excel indir"""
    file_info = await message.bot.get_file(message.document.file_id)
    tmp = tempfile.NamedTemporaryFile(delete=False, suffix=".xlsx")
    tmp.close()
    await message.bot.download_file(file_info.file_path, tmp.name)
    return Path(tmp.name)


# ===================== COMMAND =====================

@router.message(Command("block"))
async def cmd_block(message: Message, state: FSMContext):
    await state.set_state(BlockProcessingStates.waiting_for_main)
    await message.answer(
        "📄 **Blok işlemleri**\n\n"
        "2 dosyada TC=TEL eşleştirir, gruplara atar\n\n"
        "1.dosya(ana)  TC-İL-TARİH zorunlu yazılacak. (BLOK YAPISININ TEMELİ)\n\n"
        "2.dosya(tel)  TC-TEL zorunlu yazılacak."
    )


# ===================== MAIN FILE =====================

@router.message(BlockProcessingStates.waiting_for_main, F.document)
async def handle_main_excel(message: Message, state: FSMContext):
    if not message.document.file_name.endswith((".xlsx", ".xls")):
        await message.answer("❌ Sadece Excel dosyaları desteklenir")
        return

    main_path = await _download_excel(message)
    await state.update_data(main_excel=main_path)

    await state.set_state(BlockProcessingStates.waiting_for_data)
    await message.answer(
        "📄 **Veri alınacak Excel dosyasını gönderin**\n\n"
        "TC - TEL zorunlu TC ile eşleştirme yapılacaktır."
    )


# ===================== DATA FILE → MERGE → BLOCK =====================

@router.message(BlockProcessingStates.waiting_for_data, F.document)
async def handle_data_excel(message: Message, state: FSMContext):
    if not message.document.file_name.endswith((".xlsx", ".xls")):
        await message.answer("❌ Sadece Excel dosyaları desteklenir")
        return

    data = await state.get_data()
    main_excel: Path = data["main_excel"]
    data_excel = await _download_excel(message)

    loop = asyncio.get_running_loop()

    try:
        await message.answer("🔗 TC eşleştirmesi yapılıyor...")

        merged_path = await loop.run_in_executor(
            None,
            build_merged_excel,
            main_excel,
            data_excel,
            config.paths.TEMP_DIR / "merged.xlsx"
        )

        await message.answer("🧹 Excel başlıkları temizleniyor...")

        cleaner = AsyncExcelCleaner()
        cleaning = await cleaner.clean_excel_headers(str(merged_path))
        if not cleaning["success"]:
            await message.answer("❌ Excel temizleme hatası")
            await state.clear()
            return

        await message.answer("📊 Şehir blokları işleniyor...")

        splitting = await split_excel_by_blocks(
            cleaning["temp_path"],
            cleaning["headers"]
        )
        if not splitting["success"]:
            await message.answer("❌ Bloklama hatası")
            await state.clear()
            return

        await message.answer("📧 Mailler gönderiliyor...")

        mail_results = await _send_block_emails(splitting["output_files"])

        report = generate_processing_report({
            "success": True,
            "output_files": splitting["output_files"],
            "mail_results": mail_results,
            "mail_stats": calculate_mail_stats(mail_results),
            "input_filename": message.document.file_name,
            "total_rows": cleaning.get("row_count", 0),
            "matched_rows": splitting.get("matched_rows", 0),
            "unmatched_cities": splitting.get("unmatched_cities", [])
        })



        await message.answer(report)

        if config.email.PERSONAL_EMAIL:
            await send_email(
                to_emails=[config.email.PERSONAL_EMAIL],
                subject=f"📦 Blok Excel Raporu - {message.document.file_name}",
                body=report
            )

    except Exception as e:
        logger.error("❌ Block işlem hatası", exc_info=True)
        await message.answer(f"❌ İşlem hatası: {str(e)}")

    finally:
        await state.clear()


# ===================== MAIL =====================
# 4
async def _send_block_emails(output_files: Dict) -> List[Dict]:
    results = []
    
    # INPUT_EMAIL için tüm dosya bilgilerini topla
    input_email = config.email.INPUT_EMAIL
    all_files_for_input = []  # (file_path, row_count, cities) tuple listesi
    
    for group_id, file_info in output_files.items():
        if file_info["row_count"] <= 0:
            continue

        # 1) Gruplara mail gönder
        group_info = await group_manager.get_group_info(group_id)
        recipients = [r for r in group_info.get("email_recipients", []) if r]

        subject = f"{group_info.get('group_name', group_id)} - Blok Datası"
        body = (
            f"Merhaba,\n\n"
            f"{file_info['row_count']} satırlık blok veriler ekte gönderilmiştir.\n"
            f"Şehirler: {', '.join(file_info.get('cities', []))}\n\n"
            f"İyi çalışmalar,\nData_listesi_Hıdır"
        )

        for recipient in recipients:
            result = await send_email(
                to_emails=[recipient],
                subject=subject,
                body=body,
                attachments=[file_info["path"]]
            )

            # ✅ Yeni formatı kullan:
            results.append(
                build_mail_result(
                    "group",
                    bool(result and result.get("success")),
                    recipient=recipient,
                    filename=file_info["filename"],
                    error=result.get("error") if result else None
                )
            )

            await asyncio.sleep(1.2)
        
        # INPUT_EMAIL için dosya bilgilerini topla
        all_files_for_input.append({
            "path": file_info["path"],
            "row_count": file_info["row_count"],
            "cities": file_info.get("cities", []),
            "filename": file_info["filename"],
            "group_id": group_id
        })
    
    # 2) INPUT_EMAIL'e TÜM dosyaları tek mailde gönder
    if input_email and all_files_for_input:
        input_subject = f"📥 BLOK İŞLEMİ (input) Datası -"
        input_body = (
            f"Merhaba,\n\n"
            f"Blok işlemi tamamlandı. Tüm çıktı dosyaları bu mailin ekinde gönderilmiştir.\n\n"
            f"Toplam {len(all_files_for_input)} adet Excel dosyası:\n"
        )
        
        # Dosya listesini oluştur
        total_rows = 0
        for i, file_data in enumerate(all_files_for_input, 1):
            row_count = file_data["row_count"]
            cities = file_data["cities"]
            filename = file_data["filename"]
            total_rows += row_count
            
            input_body += f"{i}. {filename} - {row_count} satır"
            if cities:
                input_body += f" - Şehirler: {', '.join(cities)}"
            input_body += "\n"
        
        input_body += f"\nToplam blok sayısı: {len(all_files_for_input)}\n"
        input_body += f"Toplam satır sayısı: {total_rows}\n\n"
        input_body += "İyi çalışmalar,\nData_listesi_Hıdır"
        
        attachments = [file_data["path"] for file_data in all_files_for_input]
        
        result = await send_email(
            to_emails=[input_email],
            subject=input_subject,
            body=input_body,
            attachments=attachments
        )
        
        # ✅ INPUT mailini de aynı formatta ekle:
        results.append(
            build_mail_result(
                "input",
                bool(result and result.get("success")),
                recipient=input_email,
                filename=f"{len(all_files_for_input)}_DOSYA",
                error=result.get("error") if result else None
            )
        )
    
    return results
    
    
    