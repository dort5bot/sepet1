# utils/reporter.py
"""
TAM ASYNC Rapor Oluşturucu - GroupManager ile TAM UYUMLU
Revize Tarihi: 20-11-2025

# Yaptığı işler:
- Dosya işleme raporları oluşturur
- E-posta gönderim raporları hazırlar
- İstatistikleri okunabilir formata çevirir
- Hata durumlarında detaylı bilgi sunar
- Grup bazlı sonuçları formatlar
"""

from typing import Dict, List, Any
from datetime import datetime
from utils.group_manager import group_manager
from utils.logger import logger 

async def generate_processing_report(result: Dict) -> str:
    """✅ İşlem sonrası detaylı rapor oluşturur - HATA GÜVENLİ"""
    try:
        if not result.get("success", False):
            error_msg = result.get("error", "Bilinmeyen hata")
            # Hata mesajını kısalt
            if len(error_msg) > 500:
                error_msg = error_msg[:500] + "..."
            return f"❌ İşlem başarısız oldu:\n{error_msg}"
        
        output_files = result.get("output_files", {})
        total_rows = result.get("total_rows", 0)
        matched_rows = result.get("matched_rows", 0)
        unmatched_rows = total_rows - matched_rows
        email_results = result.get("email_results", [])
        user_id = result.get("user_id", "Bilinmeyen")
        
        successful_emails = sum(1 for res in email_results if res.get("success", False))
        failed_emails = len(email_results) - successful_emails
        
        # Toplu mail bilgisi
        bulk_email_sent = result.get("bulk_email_sent", False)
        bulk_email_recipient = result.get("bulk_email_recipient")
        
        report_lines = [
            "✅ **DOSYA İŞLEME RAPORU**",
            f"⏰ İşlem zamanı: {datetime.now().strftime('%d.%m.%Y %H:%M')}",
            #f"👤 Kullanıcı ID: {user_id}",
            "",
            "📊 **İSTATİSTİKLER:**",
            f"• Toplam satır: {total_rows}",
            f"• Eşleşen satır: {matched_rows}",
            f"• Eşleşmeyen satır: {unmatched_rows}",
            f"• Oluşturulan dosya: {len(output_files)}",
            f"• Başarılı mail: {successful_emails}",
            f"• Başarısız mail: {failed_emails}",
        ]
        
        # Toplu mail durumu
        if bulk_email_sent and bulk_email_recipient:
            report_lines.append(f"• 📧 Otomatik Rapor mail: {bulk_email_recipient} ✅")
        else:
            report_lines.append("• 📧 Otomatik Rapor mail: Gönderilemedi ❌")
        
        report_lines.extend([
            "",
            "📁 **OLUŞTURULAN DOSYALAR:**"
        ])
        
        # ✅ TAM ASYNC: Grup bilgilerini async olarak al
        for group_id, file_info in output_files.items():
            filename = file_info.get("filename", "bilinmeyen")
            row_count = file_info.get("row_count", 0)
            
            # ✅ GROUP MANAGER UYUMLU: Doğru async metod
            group_info = await group_manager.get_group_info(group_id)
            group_name = group_info.get("group_name", group_id)
            
            report_lines.append(f"• {group_name}: {filename} ({row_count} satır)")
        
        # Eşleşmeyen şehirler
        unmatched_cities = result.get("unmatched_cities", [])
        if unmatched_cities:
            report_lines.extend([
                "",
                "⚠️ **EŞLEŞMEYEN ŞEHİRLER:**",
                f"Toplam {len(unmatched_cities)} farklı şehir:"
            ])
            for city in unmatched_cities[:5]:
                report_lines.append(f"• {city}")
            if len(unmatched_cities) > 5:
                report_lines.append(f"• ... ve {len(unmatched_cities) - 5} diğer şehir")
        
        # Mail hataları
        if failed_emails > 0:
            report_lines.extend([
                "",
                "❌ **MAIL GÖNDERİM HATALARI:**"
            ])
            error_count = 0
            for error in email_results:
                if not error.get("success", False) and error_count < 3:
                    report_lines.append(f"• {error.get('recipient', 'Bilinmeyen')}: {error.get('error', 'Bilinmeyen hata')}")
                    error_count += 1
            if failed_emails > 3:
                report_lines.append(f"• ... ve {failed_emails - 3} diğer hata")
        
        return "\n".join(report_lines)
        
    except Exception as e:
        logger.error(f"Rapor oluşturma hatası: {e}")
        return f"❌ Rapor oluşturma hatası: {str(e)}"


async def generate_email_report(email_results: List[Dict]) -> str:
    """✅ Email gönderim raporu oluşturur - TAM ASYNC"""
    try:
        successful = sum(1 for res in email_results if res.get("success", False))
        failed = len(email_results) - successful
        
        report = [
            f"📧 **EMAIL RAPORU**",
            f"⏰ İşlem zamanı: {datetime.now().strftime('%d.%m.%Y %H:%M')}",
            f"✅ Başarılı: {successful}",
            f"❌ Başarısız: {failed}",
            ""
        ]
        
        if failed > 0:
            report.append("**Hatalar:**")
            for i, result in enumerate(email_results[:5], 1):
                if not result.get("success", False):
                    report.append(f"{i}. {result.get('recipient', 'Bilinmeyen')}: {result.get('error', 'Bilinmeyen hata')}")
        
        return "\n".join(report)
        
    except Exception as e:
        return f"❌ Email raporu oluşturma hatası: {str(e)}"


async def generate_personal_email_report(result: Dict) -> str:
    """✅ Kişisel mail gönderim raporu oluşturur - TAM ASYNC"""
    try:
        if not result.get("success", False):
            error_msg = result.get("error", "Bilinmeyen hata")
            return f"❌ İşlem başarısız oldu:\n{error_msg}"
        
        total_rows = result.get("total_rows", 0)
        email_sent_to = result.get("email_sent_to", "Bilinmeyen")
        user_id = result.get("user_id", "Bilinmeyen")
        
        report_lines = [
            "✅ **KİŞİSEL MAIL GÖNDERİM RAPORU**",
            f"⏰ İşlem zamanı: {datetime.now().strftime('%d.%m.%Y %H:%M')}",
            #f"👤 Kullanıcı ID: {user_id}",
            "",
            "📊 **İSTATİSTİKLER:**",
            f"• Toplam satır: {total_rows}",
            f"• Gönderilen mail: {email_sent_to}",
            "",
            "📧 **DURUM:** Mail başarıyla gönderildi! ✅"
        ]
        
        return "\n".join(report_lines)
        
    except Exception as e:
        return f"❌ Kişisel email raporu oluşturma hatası: {str(e)}"


async def generate_group_statistics_report() -> str:
    """✅ Grup istatistikleri raporu oluşturur - TAM ASYNC & GROUP MANAGER UYUMLU"""
    try:
        # ✅ GROUP MANAGER UYUMLU: Async istatistikleri al
        stats = await group_manager.get_cities_statistics()
        all_groups = await group_manager.get_all_groups()
        
        report_lines = [
            "📊 **GRUP İSTATİSTİKLERİ RAPORU**",
            f"⏰ İşlem zamanı: {datetime.now().strftime('%d.%m.%Y %H:%M')}",
            "",
            "📈 **GENEL İSTATİSTİKLER:**",
            f"• Toplam grup: {stats['total_groups']}",
            f"• Toplam şehir: {stats['total_cities']}",
            f"• Benzersiz şehir: {stats['unique_cities']}",
            f"• Mapping kayıtları: {stats['mapping_entries']}",
            "",
            "👥 **AKTİF GRUPLAR:**"
        ]
        
        active_groups = [group for group in all_groups if group.get('is_active', True)]
        for group in active_groups:
            group_id = group.get('group_id', 'Bilinmeyen')
            group_name = group.get('group_name', group_id)
            email_count = len(group.get('email_recipients', []))
            city_count = len(group.get('cities', []))
            
            report_lines.append(f"• {group_name}: {email_count} mail, {city_count} şehir")
        
        return "\n".join(report_lines)
        
    except Exception as e:
        return f"❌ İstatistik raporu oluşturma hatası: {str(e)}"