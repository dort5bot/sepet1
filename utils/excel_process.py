# utils/excel_process.py
"""
Excel işleme ve mail gönderim modülü

→  [ Excel işlemleri > paralel ]  
→  [ MAIL işlemleri → serİ gönderİm ]
→  [ TELEGRAM RAPORU → seri ]
(personal, telegram rapor yapısı reporter.py içinden ayarlanır)

- Excel işlemleri paralel çalışabilir
- Mail gönderimi HER ZAMAN seri
- Aynı mail yapısı tüm tiplerde kullanılır

işlem sırası TÜM MAİLLER EN SON GÖNDERİLİR
[1] Excel temizleme (seri)
[2] Excel split (seri)
[3] Grup mailleri (gmail seri çalışır kesinlikle paralel olmayacak)
[4] Input mail (SERİ — grup mailleri bittikten sonra)
[5] personal Rapor oluşturma → seri
[6] telegram Raporu → seri
Hiçbir mail Excel işlemleri devam ederken gönderilmez.
SMTP bağlantısı her mailde 1 kere kullanılır aç-kapat

"""

import asyncio
from pathlib import Path
from typing import Dict, Any, List
import tempfile
import zipfile
from datetime import datetime

from config import config
from utils.excel_cleaner import AsyncExcelCleaner
from utils.excel_splitter import split_excel_by_groups
from utils.reporter import generate_processing_report
from utils.mailer import send_email
from utils.group_manager import group_manager
from utils.logger import logger


# ============================================================
# YARDIMCI – ORTAK YAPILAR
# ============================================================

def build_mail_result(
    mail_type: str,
    success: bool,
    recipient=None,
    filename=None,
    **extra
) -> Dict:
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


# ============================================================
# ANA AKIŞ
# ============================================================

async def process_excel_task(input_path: Path, user_id: int) -> Dict[str, Any]:
    mail_results: List[Dict] = []
    temp_files: List[str] = []

    try:
        logger.info(f"📊 Excel işleme başlatıldı: {input_path.name}")

        cleaning_result = await _clean_excel_headers_async(str(input_path))
        if not cleaning_result["success"]:
            return {"success": False, "error": cleaning_result.get("error")}

        temp_files.append(cleaning_result["temp_path"])

        splitting_result = await split_excel_by_groups(
            cleaning_result["temp_path"],
            cleaning_result["headers"]
        )
        if not splitting_result["success"]:
            return {"success": False, "error": splitting_result.get("error")}

        output_files = splitting_result["output_files"]

        mail_results.extend(await _send_group_emails(output_files))
        mail_results.extend(await send_input_only_email(input_path))

        processing_context = {
            "success": True,
            "output_files": output_files,
            "total_rows": cleaning_result["row_count"],
            "processed_rows": splitting_result["processed_rows"],
            "matched_rows": splitting_result["matched_rows"],
            "unmatched_cities": splitting_result.get("unmatched_cities", []),
            "mail_results": mail_results,
            "mail_stats": calculate_mail_stats(mail_results),
            "input_filename": input_path.name,  # ✅ Doğrudan burada ekliyoruz
        }

        mail_results.extend(
            await _send_personal_email(input_path, output_files, processing_context)
        )

        processing_context["mail_results"] = mail_results
        processing_context["mail_stats"] = calculate_mail_stats(mail_results)

        return processing_context


    except Exception as e:
        logger.error(f"❌ İşlem hatası: {e}", exc_info=True)
        mail_results.append(build_mail_result("system", False, error=str(e)))
        return {"success": False, "error": str(e), "mail_results": mail_results}

    finally:
        await _cleanup_temp_files(temp_files)


# ============================================================
# EXCEL
# ============================================================

async def _clean_excel_headers_async(input_path: str) -> Dict[str, Any]:
    try:
        cleaner = AsyncExcelCleaner()
        return await cleaner.clean_excel_headers(input_path)
    except Exception as e:
        logger.error(f"❌ Excel temizleme hatası: {e}")
        return {"success": False, "error": str(e)}


# ============================================================
# MAIL – GROUP
# ============================================================

async def _send_group_emails(output_files: Dict) -> List[Dict]:
    await group_manager._ensure_initialized()
    mail_queue: List[Dict] = []
    results: List[Dict] = []

    for group_id, file_info in output_files.items():
        if file_info["row_count"] <= 0:
            continue

        group_info = await group_manager.get_group_info(group_id)
        recipients = [r.strip() for r in group_info.get("email_recipients", []) if r]

        subject = f"{group_info.get('group_name', group_id)} - {file_info['filename']}"
        body = (
            f"Merhaba,\n\n"
            f"{group_info.get('group_name', group_id)} grubu için "
            f"{file_info['row_count']} satırlık rapor ekte gönderilmiştir.\n\n"
            f"İyi çalışmalar,\nData_listesi_Hıdır"
        )

        for r in recipients:
            mail_queue.append({
                "recipient": r,
                "group_id": group_id,
                "filename": file_info["path"].name,
                "subject": subject,
                "body": body,
                "attachment": file_info["path"]
            })

    for mail in mail_queue:
        result = await send_email(
            to_emails=[mail["recipient"]],
            subject=mail["subject"],
            body=mail["body"],
            attachments=[mail["attachment"]]
        )

        results.append(
            build_mail_result(
                "group",
                bool(result and result.get("success")),
                recipient=mail["recipient"],
                filename=mail["filename"],
                error=result.get("error") if result else None
            )
        )

        await asyncio.sleep(1.2)

    return results


# ============================================================
# MAIL – INPUT
# ============================================================

async def send_input_only_email(input_path: Path) -> List[Dict]:
    email = getattr(config.email, "INPUT_EMAIL", None)
    if not email or not input_path.exists():
        return [build_mail_result("input", False, error="Input mail yapılandırılamadı")]

    result = await send_email(
        to_emails=[email],
        subject=f"📥 Teldata Input excel - {input_path.name}",
        body="Merhaba,\n\nTelefon data dosyası ektedir.\n\nİyi çalışmalar,\nData_listesi_Hıdır",
        attachments=[input_path]
    )


    return [
        build_mail_result(
            "input",
            bool(result and result.get("success")),
            recipient=email,
            filename=input_path.name,
            error=result.get("error") if result else None
        )
    ]


# ============================================================
# MAIL – PERSONAL
# ============================================================

async def _send_personal_email(
    input_path: Path,
    output_files: Dict,
    processing_result: Dict
) -> List[Dict]:

    email = config.email.PERSONAL_EMAIL
    if not email:
        return [build_mail_result("personal", False, error="PERSONAL_EMAIL yok")]

    zip_path = await create_backup_zip(input_path, output_files)
    # report_text = generate_processing_report(processing_result, "mail")

    report_text = generate_processing_report(processing_result)


    result = await send_email(
        to_emails=[email],
        subject=f"📦 Excel Data Raporu - {input_path.name}",
        body=f"Merhaba,\n\n{report_text}\n\nİyi çalışmalar,\nData_listesi_Hıdır",
        attachments=[zip_path]
    )

    return [
        build_mail_result(
            "personal",
            bool(result and result.get("success")),
            recipient=email,
            filename=zip_path.name if zip_path else None,
            error=result.get("error") if result else None
        )
    ]


# ============================================================
# TEMİZLİK & ZIP
# ============================================================

async def _cleanup_temp_files(files: List[str]):
    for f in files:
        try:
            Path(f).unlink(missing_ok=True)
        except Exception as e:
            logger.warning(f"⚠️ Temp silinemedi: {e}")


async def create_backup_zip(input_path: Path, output_files: Dict) -> Path:
    zip_path = Path(tempfile.gettempdir()) / f"excel_{datetime.now():%Y%m%d_%H%M%S}.zip"

    def sync_zip():
        with zipfile.ZipFile(zip_path, "w", zipfile.ZIP_DEFLATED) as z:
            if input_path.exists():
                z.write(input_path, input_path.name)
            for f in output_files.values():
                if f["path"].exists():
                    z.write(f["path"], f["filename"])
        return zip_path

    loop = asyncio.get_running_loop()
    return await loop.run_in_executor(None, sync_zip)
