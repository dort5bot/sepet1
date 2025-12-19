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
"""
Excel işlem raporunu metne çevirir.

for_internal_message=True:
- Telegram bot mesajı
- İç sistem raporu (daha detaylı)

for_internal_message=False:
- Mail raporu
- Dış paylaşıma uygun
"""


def generate_processing_report(
    result: Dict,
    for_internal_message: bool = False
) -> str:
    """
    Excel işlem raporunu metne çevirir.
    
    for_internal_message=True: Telegram bot mesajı (daha detaylı)
    for_internal_message=False: Mail raporu (dış paylaşıma uygun)
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
        
        # input_filename = result.get("input_filename", "Bilinmeyen Dosya")
        # main_excel_name = result.get("main_excel_name", " ")
        
        main_excel_name = result.get("main_excel_name", "Bilinmeyen Dosya")
        input_filename = result.get("input_filename", main_excel_name)
        
        # YENİ: Tüm işlenen şehirleri topla
        all_cities = set()
        for group_id, file_info in output_files.items():
            cities = file_info.get("cities", [])
            if isinstance(cities, list):
                all_cities.update(cities)
        
        city_count = len(all_cities)
        cities_list = sorted(list(all_cities))
        
        # -------------------------------------------------
        # 2) HATALARI AYRIŞTIR
        # -------------------------------------------------
        mail_errors = [
            res for res in mail_results 
            if not res.get("success", False)
        ]
        
        # -------------------------------------------------
        # 3) GRUP DOSYALARINI HAZIRLA
        # -------------------------------------------------
        groups_list = []
        for group_id, file_info in output_files.items():
            groups_list.append({
                "group_id": group_id,
                "group_name": group_id,
                "filename": file_info.get("filename", ""),
                "row_count": file_info.get("row_count", 0)
            })
        
        # -------------------------------------------------
        # 4) ORTAK RAPOR BAŞLIĞI
        # -------------------------------------------------
        report_lines = [
            f"✅  __ EXCEL İŞLEM RAPORU __\n\n"
            f"⏰  İşlem zamanı: {datetime.now().strftime('%d.%m.%Y %H:%M')}",
            f"📄  Dosya:  {main_excel_name} - {input_filename}",
            "", # Blok işleminde 1. dosya adı gelir
        ]
        # Eğer blok işlemiyse ve dosya adları farklıysa, ek bilgi
        # if main_excel_name != input_filename:
        #     report_lines.insert(3, f"📄  İşlenen dosya: {input_filename}")
        #     report_lines.insert(4, "")  # boşluk ekle
        #     
        # -------------------------------------------------
        # 5) İSTATİSTİKLER (HER İKİ RAPOR İÇİN)
        # -------------------------------------------------
        report_lines.extend([
            f"📊  İstatistikler:",
            f"• Excel (input) satır: {total_rows}",
            f"• Oluşan grup dosyası: {len(groups_list)}",
            f"• Dosyadaki il sayısı: {city_count}",
            "",
            f"📧  Mail Gönderim: ({mail_stats.get('total', 0)} tane)",
            f"• Grup | Input : {mail_stats.get('by_type', {}).get('group_sent', 0)} | {mail_stats.get('by_type', {}).get('input_sent', 0)}",
            f"• Başarısız: {mail_stats.get('failed', 0)}",
        ])
        
        # -------------------------------------------------
        # 6) TELEGRAM RAPORU İÇİN EKSTRA
        # -------------------------------------------------
        # if for_internal_message:
            # report_lines.append(
                # f"• Personal: {mail_stats.get('by_type', {}).get('personal_sent', 0)}",
                # f"• Dosyadaki iller: {cities_list}",
            # )

        # -------------------------------------------------
        # 6) TELEGRAM RAPORU İÇİN EKSTRA
        # -------------------------------------------------
        if for_internal_message:
            report_lines.extend([
                f"• Personal: {mail_stats.get('by_type', {}).get('personal_sent', 0)}",
                f"• Dosyadaki iller: {', '.join(cities_list) if cities_list else 'Yok'}",
            ])

        # -------------------------------------------------
        # 7) ŞEHİR LİSTESİ (MAIL RAPORU İÇİN)
        # -------------------------------------------------
        if not for_internal_message and all_cities:
            report_lines.append("")
            report_lines.append("🏙️  **Dosyadaki iller:**")
            
            if cities_list:
                # Şehirleri 5'li gruplar halinde göster
                for i in range(0, len(cities_list), 9):
                    chunk = cities_list[i:i+9]
                    report_lines.append(f"• {', '.join(chunk)}")
                
                if city_count > 73:
                    report_lines.append(f"• ... ve {city_count - 73} diğer şehir")
        
        # -------------------------------------------------
        # 8) GRUP DOSYALARI
        # -------------------------------------------------
        report_lines.append("")
        report_lines.append(f"📁  Grup Dosyaları: ({len(groups_list)} tane)")
        
        for g in groups_list:
            report_lines.append(
                f"• {g.get('group_name', g.get('group_id'))}: "
                f"{g.get('filename')} ({g.get('row_count', 0)} satır)"
            )
        
        # -------------------------------------------------
        # 9) EŞLEŞMEYEN ŞEHİRLER
        # -------------------------------------------------
        if unmatched_cities:
            report_lines.extend([
                "",
                f"⚠️  Eşleşmeyen Şehirler: ({len(unmatched_cities)} tane)",
            ])
            for city in unmatched_cities[:3]:
                report_lines.append(f"• {city}")
            if len(unmatched_cities) > 3:
                report_lines.append(
                    f"• ... ve {len(unmatched_cities) - 3} diğer şehir"
                )
        
        # -------------------------------------------------
        # 10) MAIL HATALARI
        # -------------------------------------------------
        if mail_errors:
            report_lines.extend(["", "❌  Mail Gönderim Hataları:"])
            for err in mail_errors[:8]:
                report_lines.append(
                    f"• {err.get('mail_type')} -> "
                    f"{err.get('recipient')}: {err.get('error')}"
                )
        
        return "\n".join(report_lines)
        
    except Exception as e:
        logger.error("Rapor oluşturma hatası", exc_info=True)
        return f"❌ Rapor oluşturma hatası: {str(e)}"