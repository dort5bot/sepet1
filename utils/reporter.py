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

#✅ İşlem sonrası detaylı rapor oluşturur report_type: "mail" veya "telegram"


from datetime import datetime
from typing import Dict
from utils.logger import logger



# utils/excel_process.py için yardımcı raporlama metodu
# utils/reporter.py
# -----------------------------------------------------
def generate_processing_report(
    result: Dict,
    for_internal_message: bool = False
) -> str:
    """
    Excel işlem raporunu metne çevirir.

    for_internal_message=True:
        - Telegram bot mesajı
        - İç sistem raporu (daha detaylı)

    for_internal_message=False:
        - Mail raporu
        - Dış paylaşıma uygun
    """
    try:
        # -------------------------------------------------
        # 0) HATA DURUMU
        # -------------------------------------------------
        if not result.get("success", False):
            error_msg = result.get("error", "Bilinmeyen hata")
            if len(error_msg) > 500:
                error_msg = error_msg[:500] + "..."
            return f"❌ İşlem başarısız oldu:\n{error_msg}"

        # -------------------------------------------------
        # 1) VERİLER
        # -------------------------------------------------
        total_rows = result.get("total_rows", 0)
        matched_rows = result.get("matched_rows", 0)
        output_files = result.get("output_files", {})
        unmatched_cities = result.get("unmatched_cities", [])
        mail_stats = result.get("mail_stats", {})
        mail_results = result.get("mail_results", [])
        input_filename = result.get("input_filename", "Bilinmeyen Dosya") 
       
        # 2) HATALARI AYRIŞTIR
        # -------------------------------------------------
        mail_errors = [
            res for res in mail_results 
            if not res.get("success", False)
        ]
        
        # 3) GRUP DOSYALARINI HAZIRLA
        # -------------------------------------------------
        groups_list = []
        for group_id, file_info in output_files.items():
            groups_list.append({
                "group_id": group_id,
                "group_name": group_id,  # group_manager'dan alınabilir
                "filename": file_info.get("filename", ""),
                "row_count": file_info.get("row_count", 0)
            })
            
        # -------------------------------------------------
        # 4) ORTAK RAPOR
        # -------------------------------------------------
        report_lines = [
            f"✅  __ EXCEL İŞLEM RAPORU __\n\n"
            f"⏰  İşlem zamanı: {datetime.now().strftime('%d.%m.%Y %H:%M')}",
            f"• Dosya: {input_filename}",
            "",
            # "📊  İstatistikler",
            f"• Excel (input) satır: {total_rows}",
            f"• Eşleşen satır: {matched_rows}",
            f"• Oluşan grup dosyası: {len(groups_list)}",
            "",
            f"📧  Mail Gönderim: ({mail_stats.get('total', 0)} tane)",
            # f"• Başarılı: {mail_stats.get('sent', 0)}",
            f"• Grup | Input : {mail_stats.get('by_type', {}).get('group_sent', 0)} | {mail_stats.get('by_type', {}).get('input_sent', 0)}",
            f"• Başarısız: {mail_stats.get('failed', 0)}",
        ]

        # -------------------------------------------------
        # 4-2) SADECE INTERNAL (Telegram bot mesajı)
        # -------------------------------------------------
        if for_internal_message:
            report_lines.append(
                f"• Personal: {mail_stats.get('by_type', {}).get('personal_sent', 0)}"
            )


        # -------------------------------------------------
        # 5) GRUP DOSYALARI
        # -------------------------------------------------
        report_lines.append("")
        report_lines.append(f"📁 *Grup Dosyaları: ({len(groups_list)} tane)*")

        for g in groups_list:
            report_lines.append(
                f"• {g.get('group_name', g.get('group_id'))}: "
                f"{g.get('filename')} ({g.get('row_count', 0)} satır)"
            )

        # -------------------------------------------------
        # 6) EŞLEŞMEYEN ŞEHİRLER
        # -------------------------------------------------
        if unmatched_cities:
            report_lines.extend([
                "",
                f"⚠️ **Eşleşmeyen Şehirler: ({len(unmatched_cities)} tane)**",
            ])
            for city in unmatched_cities[:3]:
                report_lines.append(f"• {city}")
            if len(unmatched_cities) > 3:
                report_lines.append(
                    f"• ... ve {len(unmatched_cities) - 3} diğer şehir"
                )

        # -------------------------------------------------
        # 7) MAIL HATALARI (HER İKİSİNDE DE GÖSTERİLEBİLİR)
        # -------------------------------------------------
        if mail_errors:
            report_lines.extend(["", "❌ **Mail Gönderim Hataları:**"])
            for err in mail_errors[:8]:
                report_lines.append(
                    f"• {err.get('mail_type')} -> "
                    f"{err.get('recipient')}: {err.get('error')}"
                )

        return "\n".join(report_lines)

    except Exception as e:
        logger.error("Rapor oluşturma hatası", exc_info=True)
        return f"❌ Rapor oluşturma hatası: {str(e)}"


"""
# Grup istatistikleri raporu
# -----------------------------------------------------
# kullanılmıyor
# ✅ Grup istatistikleri raporu oluşturur - TAM ASYNC & GROUP MANAGER UYUMLU
async def generate_group_statistics_report() -> str:

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


# ======Bunlar belirsiz ======================================
# kullanılmıyor
# ✅ Email gönderim raporu oluşturur - TAM ASYNC
async def generate_email_report(mail_results: List[Dict]) -> str:
    try:
        if not mail_results:
            return "📭 Gönderilen mail bulunamadı"
            
        successful = sum(1 for res in mail_results if res.get("success", False))
        failed = len(mail_results) - successful
        
        # Türlere göre grupla
        group_emails = [r for r in mail_results if r.get("mail_type") == "group"]
        input_emails = [r for r in mail_results if r.get("mail_type") == "input"]
        personal_emails = [r for r in mail_results if r.get("mail_type") == "personal"]
        
        report = [
            f"📧 **EMAIL GÖNDERİM RAPORU**",
            f"⏰ İşlem zamanı: {datetime.now().strftime('%d.%m.%Y %H:%M')}",
            f"📊 Toplam Mail: {len(mail_results)}",
            f"✅ Başarılı: {successful}",
            f"❌ Başarısız: {failed}",
            "",
            "📋 *Dağılım:*",
            f"• Grup Mailleri: {len(group_emails)}",
            f"• Input Mailleri: {len(input_emails)}",
            f"• Personal Mailleri: {len(personal_emails)}",
            ""
        ]
        
        # Hata listesi      
        if failed > 0:
            report.append("**Hatalar:**")
            failed_results = [r for r in mail_results if not r.get("success", False)][:5]
            for i, fail in enumerate(failed_results, 1):
                recipient = fail.get('recipient', 'Bilinmeyen')
                error = fail.get('error', 'Bilinmeyen hata')
                mail_type = fail.get('mail_type', 'bilinmeyen')
                report.append(f"{i}. {mail_type} -> {recipient}: {error}")
              
        return "\n".join(report)
        
    except Exception as e:
        return f"❌ Email raporu oluşturma hatası: {str(e)}"

# kullanılmıyor
# ✅ Kişisel mail gönderim raporu oluşturur - TAM ASYNC
async def generate_personal_email_report(result: Dict) -> str:

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

"""