# utils/excel_process.py 
"""
ZIP içinde klasör ayrımı olmadan, 
tüm input ve output Excel dosyalarını
 aynı klasörde (düz olarak) bir arada zip yapar,
 belirtilen maile gönderir
 excel > grıp > mail
 amaç:
gelen + giden excelleri topluca zip mail atabilir
 
 temizlik öncesi yedekleme gibi işlevi var.
 gerekirse gelştirilebilir
20-11-2025

Amaç: Excel dosyalarını işleyip gruplara ayırır

İşlevler:
process_excel_task(): Ana işlem akışını yönetir
_send_group_emails(): Grup e-postalarını gönderir
_send_bulk_email(): Toplu e-posta gönderimini başlatır
Özellik: Excel temizleme, gruplara ayırma, mailer'ı kullanma

"""

# excel_process.py - TAM ASYNC & TAM UYUMLU VERSİYON
import asyncio
from pathlib import Path
from typing import Dict, Any, List, Optional
import tempfile
import zipfile
from datetime import datetime

from config import config
from utils.excel_cleaner import AsyncExcelCleaner
from utils.excel_splitter import split_excel_by_groups
from utils.reporter import generate_processing_report

from utils.mailer import send_email, EmailConfig, EmailAttachment

from utils.group_manager import group_manager
from utils.logger import logger


# işlem sırası TÜM MAİLLER EN SON GÖNDERİLİR
# [1] Excel temizleme (seri)
# [2] Excel split (seri)
# [3] Grup mailleri (PARALEL pytonda, gmail seri çalışır)
# [4] Input mail (SERİ — grup mailleri bittikten sonra)
# [5] Rapor oluşturma (SERİ)
# [6] Rapor maili → (İstersen seri, istersen telegram paralel)
# Hiçbir mail Excel işlemleri devam ederken gönderilmez.
# SMTP bağlantısı sadece 1 kere kullanılır (deadlock yok)
# EXCEL İŞLE | → | TÜM MAİLLERİ SIRAYLA GÖNDER | → | TELEGRAM RAPORU

async def process_excel_task(input_path: Path, user_id: int) -> Dict[str, Any]:
    """Excel işleme görevini TAM ASYNC + TAM MAIL SIRASI ile yürütür"""

    cleaning_result = None
    temp_files_to_cleanup = []

    try:
        logger.info(f"📊 Excel işleme başlatıldı: {input_path.name}, Kullanıcı: {user_id}")

        # ---------------------------------------------------------------------
        # 1) EXCEL TEMİZLEME - seri
        # ---------------------------------------------------------------------
        logger.info("TEMIZLEME BASLIYOR...11")
        cleaning_result = await _clean_excel_headers_async(str(input_path))
        logger.info("TEMIZLEME BITTI-12")

        if not cleaning_result["success"]:
            error_msg = f"Excel temizleme hatası: {cleaning_result.get('error')}"
            logger.error(error_msg)
            return {"success": False, "error": error_msg}

        temp_files_to_cleanup.append(cleaning_result["temp_path"])
        logger.info(f"✅ Excel temizlendi: {cleaning_result['row_count']} satır")


        # ---------------------------------------------------------------------
        # 2) EXCEL GRUPLAMA - seri
        # ---------------------------------------------------------------------
        logger.info("AYIRMA BASLIYOR...21")
        splitting_result = await split_excel_by_groups(
            cleaning_result["temp_path"],
            cleaning_result["headers"]
        )
        logger.info("AYIRMA BITTI-22")

        if not splitting_result["success"]:
            error_msg = f"Excel ayırma hatası: {splitting_result.get('error')}"
            logger.error(error_msg)
            return {"success": False, "error": error_msg}

        output_files = splitting_result["output_files"]
        logger.info(f"✅ Excel gruplara ayrıldı: {splitting_result['total_rows']} satır, {len(output_files)} grup")


        # ---------------------------------------------------------------------
        # 3) TÜM MAİLLER BU AŞAMADAN SONRA SIRAYLA GİDECEK
        # ---------------------------------------------------------------------

        mail_results = {
            "group_mails": {}
            # "input_mail": None,
            # "personal_mail": None
        }

        # ************************************************************
        # 3.1 GRUP MAİLLERİ (N adet) - paralel
        # ************************************************************
        logger.info("📧 GRUP MAİLLERİ GÖNDERİLİYOR... (1/3)")
        group_results = await _send_group_emails(output_files)
        mail_results["group_mails"] = group_results


        # ************************************************************
        # 3.2 INPUT MAIL - seri
        # ************************************************************
        logger.info("📧 INPUT MAIL GÖNDERİLİYOR... (2/3)")
        mail_results["input_mail"] = await send_input_only_email(input_path)
        #MAİLİ GÖRMEK İSTERSEN 
        mail_results["input_email_recipient"] = getattr(config.email, "INPUT_EMAIL", None)



        # ************************************************************
        # 3.3 KİŞİSEL MAIL (ZIP + RAPOR) – EN SON - seri
        # ************************************************************
        logger.info("📧 KİŞİSEL SON RAPOR MAILİ GÖNDERİLİYOR... (3/3)")
        mail_results["personal_mail"] = await _send_bulk_email(
            input_path,
            output_files,
            {
                "success": True,
                "output_files": output_files,
                "total_rows": splitting_result["total_rows"],
                "matched_rows": splitting_result["matched_rows"],
                "unmatched_cities": splitting_result.get("unmatched_cities", []),
                #"group_emails": group_results,
                "email_results": group_results,
                "input_email_sent": mail_results["input_mail"],
            }
        )
        #mail adresini görmek İstersen 
        mail_results["bulk_email_recipient"] = getattr(config.email, "PERSONAL_EMAIL", None)



        # ---------------------------------------------------------------------
        # 4) TELEGRAM RAPORU
        # ---------------------------------------------------------------------
        final_result = {
            "success": True,
            "output_files": output_files,
            "total_rows": splitting_result["total_rows"],
            "matched_rows": splitting_result["matched_rows"],
            "unmatched_cities": splitting_result.get("unmatched_cities", []),
            "stats": splitting_result.get("stats", {}),
            "mail_results": mail_results,
            "user_id": user_id,
            
            "input_email_recipient": mail_results["input_email_recipient"],
            "bulk_email_recipient": mail_results["bulk_email_recipient"],
           
            "email_results": group_results,  # _send_group_emails'dan gelen sonuçları direkt kullanın
            "input_email_sent": mail_results.get("input_mail", False),  # .get ile güvenli erişim
            "bulk_email_sent": mail_results.get("personal_mail", False),  # .get ile güvenli erişim

    
            # # ✅ YENİ EKLENEN ALANLAR:
            # "email_results": mail_results["group_mails"],  # Grup mail sonuçları
            # "input_email_sent": mail_results["input_mail"],  # Input mail durumu
            # "bulk_email_sent": mail_results["personal_mail"],  # Toplu mail durumu

        }

        telegram_report = await generate_processing_report(final_result, "telegram")
        logger.info(f"📱 Telegram raporu hazır: {len(telegram_report)} karakter")

        return final_result

    except Exception as e:
        logger.error(f"❌ İşlem görevi hatası: {e}", exc_info=True)
        return {"success": False, "error": str(e)}

    finally:
        await _cleanup_temp_files(temp_files_to_cleanup)


async def _clean_excel_headers_async(input_path: str) -> Dict[str, Any]:
    """Excel temizleme işlemini TAM ASYNC olarak yürütür"""
    try:
        cleaner = AsyncExcelCleaner()
        result = await cleaner.clean_excel_headers(input_path)
        return result
    except Exception as e:
        logger.error(f"❌ Async Excel temizleme hatası: {e}")
        return {"success": False, "error": str(e)}

async def _send_group_emails(output_files: Dict) -> List[Dict]:
    """Grup maillerini TAM ASYNC olarak gönderir"""
    email_tasks = []
    email_results = []
    
    try:
        # Group manager'ın başlatıldığından emin ol
        # ---------------------------------------------
        await group_manager._ensure_initialized()
        
        for group_id, file_info in output_files.items():
            if file_info["row_count"] <= 0:
                logger.warning(f"📭 Boş dosya atlandı: {group_id}")
                continue
                
            group_info = await group_manager.get_group_info(group_id)
            recipients = group_info.get("email_recipients", [])
            
            if not recipients:
                logger.warning(f"📭 Alıcı bulunamadı: {group_id}")
                continue
            
            # Geçerli email adreslerini filtrele
            # ---------------------------------------------
            valid_recipients = [
                recipient.strip() for recipient in recipients 
                if recipient and recipient.strip()
            ]
            
            if not valid_recipients:
                logger.warning(f"📭 Geçerli alıcı bulunamadı: {group_id}")
                continue
            
            # subject = f"{group_info.get('group_name', group_id)} Raporu - {file_info['filename']}"
            subject = f"{group_info.get('group_name', group_id)} - {file_info['filename']}"
            body = (
                f"Merhaba,\n\n"
                f"{group_info.get('group_name', group_id)} grubu için {file_info['row_count']} satırlık rapor ekte gönderilmiştir.\n\n"
                f"İyi çalışmalar,\nData_listesi_Hıdır"
            )
            
            # Her alıcı için mail görevi oluştur
            # ---------------------------------------------
            for recipient in valid_recipients:
                #  send_email fonksiyonunu doğrudan kullan
                task = send_email(
                    to_emails=[recipient],
                    subject=subject,
                    body=body,
                    attachments=[file_info["path"]]
                )
                email_tasks.append((task, group_id, recipient, file_info["path"].name))
        
        if not email_tasks:
            logger.info("📭 Gönderilecek mail görevi bulunamadı")
            return []
        
        logger.info(f"📧 {len(email_tasks)} mail görevi başlatılıyor...")
        
        # Tüm mail görevlerini paralel çalıştır
        # ---------------------------------------------
        tasks = [task[0] for task in email_tasks]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        # Sonuçları işle
        # ---------------------------------------------
        for i, result in enumerate(results):
            task_info = email_tasks[i]
            group_id, recipient, filename = task_info[1], task_info[2], task_info[3]
            
            if isinstance(result, Exception):
                logger.error(f"❌ Mail gönderim hatası - Grup: {group_id}, Alıcı: {recipient}, Dosya: {filename}, Hata: {result}")
                email_results.append({
                    "success": False,
                    "group_id": group_id,
                    "recipient": recipient,
                    "filename": filename,
                    "error": str(result)
                })
            elif result and result.get("success"):
                logger.info(f"✅ Mail gönderildi - Grup: {group_id}, Alıcı: {recipient}, Dosya: {filename}")
                email_results.append({
                    "success": True,
                    "group_id": group_id,
                    "recipient": recipient,
                    "filename": filename,
                    "port_used": result.get("port_used")
                })
            else:
                logger.error(f"❌ Mail gönderilemedi - Grup: {group_id}, Alıcı: {recipient}, Dosya: {filename}")
                email_results.append({
                    "success": False,
                    "group_id": group_id,
                    "recipient": recipient,
                    "filename": filename,
                    "error": result.get("error") if result else "Gönderim başarısız"
                })
        
        # İstatistikleri logla
        # ---------------------------------------------
        successful_emails = sum(1 for result in email_results if result.get("success"))
        logger.info(f"📊 Mail gönderim istatistiği: {successful_emails}/{len(email_results)} başarılı")
        
        return email_results
        
    except Exception as e:
        logger.error(f"❌ Grup mail gönderim hatası: {e}", exc_info=True)
        return [{"success": False, "error": str(e)}]


async def _send_bulk_email(input_path: Path, output_files: Dict, processing_result: Dict) -> bool:
    """Toplu mail gönderimini TAM ASYNC olarak yönetir"""
    try:
        if not config.email.PERSONAL_EMAIL:
            logger.error("❌ PERSONAL_EMAIL tanımlı değil")
            return False
            
        logger.info(f"📦 Toplu mail hazırlanıyor: {len(output_files)} dosya")
        
        # ZIP dosyasını oluştur
        # ---------------------------------------------
        zip_path = await create_backup_zip(input_path, output_files)
        if not zip_path:
            return False
            
        # Rapor metnini hazırla
        # ---------------------------------------------
        report_text = await generate_processing_report(processing_result, "mail")
        
        # send_email fonksiyonunu kullan
        result = await send_email(
            to_emails=[config.email.PERSONAL_EMAIL],
            # subject=f"📦 Excel Data Raporu - {datetime.now().strftime('%Y-%m-%d %H:%M')}",
            # subject = f"{group_info.get('group_name', group_id)} - {file_info['filename']}"
            subject=f"📦 Excel Data Raporu - {input_path.name}",
            
            body=f"Merhaba,\n\n{report_text}\n\nİyi çalışmalar,\nData_listesi_Hıdır",
            attachments=[zip_path]
        )
        
        # ZIP dosyasını temizle
        # ---------------------------------------------
        try:
            if zip_path.exists():
                zip_path.unlink()
                logger.info(f"🗑️ Geçici ZIP dosyası silindi: {zip_path}")
        except Exception as e:
            logger.warning(f"⚠️ ZIP dosyası silinemedi: {e}")
        
        if result and result.get("success"):
            logger.info(f"✅ Toplu mail gönderildi: {config.email.PERSONAL_EMAIL}")
            return True
        else:
            logger.error(f"❌ Toplu mail gönderilemedi: {config.email.PERSONAL_EMAIL}")
            return False
            
    except Exception as e:
        logger.error(f"❌ Toplu mail hatası: {e}", exc_info=True)
        return False


async def send_input_only_email(input_path: Path, max_retries: int = 2) -> bool:
    """Input dosyasını mail olarak gönder"""
    try:
        input_email = getattr(config.email, 'INPUT_EMAIL', None)
        if not input_email:
            logger.info("📭 INPUT_EMAIL tanımlı değil, input mail atlanıyor")
            return False
        if not input_path or not input_path.exists():
            logger.error(f"❌ Input dosyası bulunamadı: {input_path}")
            return False

        subject = f"📥 Teldata Input excel - {input_path.name}"
        body = (f"Merhaba,\n\nTelefon data dosyası ektedir.\n"
                f"Dosya: {input_path.name}\n\nİyi çalışmalar,\nData_listesi_Hıdır")
        
        # send_email fonksiyonunu kullan
        result = await send_email(
            to_emails=[input_email],
            subject=subject,
            body=body,
            attachments=[input_path]
        )
        
        return bool(result and result.get("success"))
        
    except Exception as e:
        logger.error(f"❌ Input mail gönderim hatası: {e}")
        return False

async def _cleanup_temp_files(temp_files: List[str]):
    """Geçici dosyaları TAM ASYNC olarak temizler"""
    if not temp_files:
        return
        
    cleanup_tasks = []
    
    for temp_file in temp_files:
        try:
            temp_path = Path(temp_file)
            if temp_path.exists():
                # Dosya silme işlemini async yap
                def sync_delete():
                    try:
                        temp_path.unlink()
                        return True
                    except Exception as e:
                        logger.warning(f"⚠️ Geçici dosya silinemedi {temp_file}: {e}")
                        return False
                
                loop = asyncio.get_event_loop()
                task = loop.run_in_executor(None, sync_delete)
                cleanup_tasks.append((task, temp_path.name))
                
        except Exception as e:
            logger.warning(f"⚠️ Geçici dosya temizleme hatası {temp_file}: {e}")
    
    if cleanup_tasks:
        # Tüm silme işlemlerini bekleyerek paralel çalıştır
        tasks = [task[0] for task in cleanup_tasks]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        # Sonuçları logla
        for i, result in enumerate(results):
            filename = cleanup_tasks[i][1]
            if isinstance(result, Exception) or result is False:
                logger.warning(f"⚠️ Geçici dosya silinemedi: {filename}")
            else:
                logger.info(f"🗑️ Geçici dosya silindi: {filename}")


async def process_multiple_excel_files(file_paths: List[Path], user_id: int) -> Dict[str, Any]:
    """
    Birden fazla Excel dosyasını TAM ASYNC olarak işler
    
    Args:
        file_paths: İşlenecek Excel dosya yolları listesi
        user_id: Kullanıcı ID'si
        
    Returns:
        Toplu işlem sonuçları
    """
    try:
        if not file_paths:
            return {"success": False, "error": "Dosya listesi boş"}
        
        logger.info(f"🔄 Toplu Excel işleme başlatıldı: {len(file_paths)} dosya")
        
        # Tüm dosyaları paralel işle
        tasks = [process_excel_task(file_path, user_id) for file_path in file_paths]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        # Sonuçları analiz et
        successful = []
        failed = []
        total_rows = 0
        total_emails = 0
        
        for i, result in enumerate(results):
            file_path = file_paths[i]
            
            if isinstance(result, Exception):
                failed.append({
                    "file": file_path.name,
                    "error": str(result)
                })
                continue
                
            if result.get("success"):
                successful.append({
                    "file": file_path.name,
                    "output_files": len(result.get("output_files", {})),
                    "total_rows": result.get("total_rows", 0),
                    "emails_sent": len([r for r in result.get("email_results", []) if r.get("success")])
                })
                total_rows += result.get("total_rows", 0)
                total_emails += len([r for r in result.get("email_results", []) if r.get("success")])
            else:
                failed.append({
                    "file": file_path.name,
                    "error": result.get("error", "Bilinmeyen hata")
                })
        
        return {
            "success": True,
            "total_files": len(file_paths),
            "successful_files": len(successful),
            "failed_files": len(failed),
            "total_rows_processed": total_rows,
            "total_emails_sent": total_emails,
            "successful": successful,
            "failed": failed,
            "success_rate": (len(successful) / len(file_paths)) * 100 if file_paths else 0
        }
        
    except Exception as e:
        logger.error(f"❌ Toplu Excel işleme hatası: {e}", exc_info=True)
        return {
            "success": False,
            "error": str(e),
            "total_files": len(file_paths),
            "successful_files": 0,
            "failed_files": len(file_paths)
        }


async def create_backup_zip(input_path: Path, output_files: Dict) -> Path:
    """
    Input ve output dosyalarını TAM ASYNC olarak ZIP'ler
    
    Args:
        input_path: Orijinal input dosyası
        output_files: Oluşturulan output dosyaları
        
    Returns:
        ZIP dosyasının yolu
    """
    try:
        zip_path = Path(tempfile.gettempdir()) / f"backup_{datetime.now().strftime('%Y%m%d_%H%M%S')}.zip"
        
        def sync_create_zip():
            with zipfile.ZipFile(zip_path, 'w', zipfile.ZIP_DEFLATED) as zipf:
                # Input dosyasını ekle
                if input_path.exists():
                    zipf.write(input_path, input_path.name)
                
                # Output dosyalarını ekle
                for group_id, file_info in output_files.items():
                    if file_info["path"].exists():
                        zipf.write(file_info["path"], file_info["filename"])
            
            return zip_path
        
        # ZIP oluşturma işlemini async yap
        loop = asyncio.get_event_loop()
        result_path = await loop.run_in_executor(None, sync_create_zip)
        
        logger.info(f"✅ Backup ZIP oluşturuldu: {result_path}")
        return result_path
        
    except Exception as e:
        logger.error(f"❌ Backup ZIP oluşturma hatası: {e}")
        return None
        