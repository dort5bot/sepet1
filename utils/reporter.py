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


# ESKİ
"""async def generate_processing_report(result: Dict, report_type: str = "mail") -> str:
    try:
        if not result.get("success", False):
            error_msg = result.get("error", "Bilinmeyen hata")
            if len(error_msg) > 500:
                error_msg = error_msg[:500] + "..."
            return f"❌ İşlem başarısız oldu:\n{error_msg}"
        
        output_files = result.get("output_files", {})
        total_rows = result.get("total_rows", 0)
        matched_rows = result.get("matched_rows", 0)
        unmatched_rows = total_rows - matched_rows
        email_results = result.get("email_results", [])
        
       

        # # Başarı / hata hesaplama
        # successful_group_emails = sum(1 for res in email_list if res.get("success", False))
        # failed_group_emails = len(email_list) - successful_group_emails


        # HER İKİ RAPOR TİPİ İÇİN → SADECE GRUP MAİLLERİ SAYILIR
        group_mails = result.get("mail_results", {}).get("group_mails", [])

        successful_group_emails = sum(1 for res in group_mails if res.get("success", False))
        failed_group_emails = len(group_mails) - successful_group_emails


                
        
        # Grup mail istatistikleri
        successful_group_emails = sum(1 for res in email_results if res.get("success", False))
        failed_group_emails = len(email_results) - successful_group_emails
        
        # Toplu mail bilgisi
        bulk_email_sent = result.get("bulk_email_sent", False)
        bulk_email_recipient = result.get("bulk_email_recipient")
        
        # 🆕 INPUT MAIL BİLGİSİ
        input_email_sent = result.get("input_email_sent", False)
        input_email_recipient = result.get("input_email_recipient")
        
        # ✅ RAPOR TÜRÜNE GÖRE HESAPLAMA
        if report_type == "telegram":
            # TELEGRAM: Tüm mailleri say (input + grup + toplu)
            total_successful = successful_group_emails
            total_failed = failed_group_emails
            
            if input_email_sent:
                total_successful += 1
            elif input_email_recipient:  # Input mail başarısızsa
                total_failed += 1
                
            if bulk_email_sent:
                total_successful += 1
            elif bulk_email_recipient:  # Toplu mail başarısızsa
                total_failed += 1
        else:
            # MAIL: Sadece grup mailleri
            total_successful = successful_group_emails
            total_failed = failed_group_emails
        
        report_lines = [
            "✅ **EXCEL DOSYA İŞLEME RAPORU_rp**",
            f"⏰ İşlem zamanı: {datetime.now().strftime('%d.%m.%Y %H:%M')}",
            "",
            "📊 *İstatistikler*",
            f"• Toplam satır: {total_rows}",
            # f"• Eşleşen satır: {matched_rows}",
            # f"• Eşleşmeyen satır: {unmatched_rows}",
            f"• Oluşturulan dosya: {len(output_files)}",
            f"• Başarılı mail: {total_successful}",
            f"• Başarısız mail: {total_failed}",
        ]
        
        # 🆕 INPUT MAIL DURUMU
        if input_email_sent and input_email_recipient:
            #report_lines.append(f"• 📥 Input Maili: {input_email_recipient} ✅")  # email görünmesi için kod yapısı {input_email_recipient} EKLENİR
            report_lines.append(f"• 📥 Input Maili: ✅")
        elif input_email_recipient:  # Input mail tanımlı ama gönderilememiş
            report_lines.append(f"• 📥 Input Maili: ❌")
        
        # ✅ TELEGRAM İÇİN TOPLU MAIL DURUMU
        if report_type == "telegram":
            if bulk_email_sent and bulk_email_recipient:
                report_lines.append(f"• 📧 Toplu Rapor Maili: {bulk_email_recipient} ✅")
            elif bulk_email_recipient:  # Toplu mail tanımlı ama gönderilememiş
                report_lines.append(f"• 📧 Toplu Rapor Maili: {bulk_email_recipient} ❌")
        
        report_lines.extend([
            "",
            "📁 *Grup Dosyaları:*"
        ])
        
        # ✅ TAM ASYNC: Grup bilgilerini async olarak al
        for group_id, file_info in output_files.items():
            filename = file_info.get("filename", "bilinmeyen")
            row_count = file_info.get("row_count", 0)
            
            group_info = await group_manager.get_group_info(group_id)
            group_name = group_info.get("group_name", group_id)
            
            report_lines.append(f"• {group_name}: {filename} ({row_count} satır)")
        
        # Eşleşmeyen şehirler
        unmatched_cities = result.get("unmatched_cities", [])
        if unmatched_cities:
            report_lines.extend([
                "",
                "⚠️ **Excel Eşleşmeyen Iller:**",
                f"Toplam {len(unmatched_cities)} farklı şehir:"
            ])
            for city in unmatched_cities[:3]:
                report_lines.append(f"• {city}")
            if len(unmatched_cities) > 3:
                report_lines.append(f"• ... ve {len(unmatched_cities) - 3} diğer şehir")
        
        # Mail hataları
        if failed_group_emails > 0:
            report_lines.extend([
                "",
                "❌ **MAIL GÖNDERİM HATALARI:**"
            ])
            error_count = 0
            for error in email_results:
                if not error.get("success", False) and error_count < 3:
                    report_lines.append(f"• {error.get('recipient', 'Bilinmeyen')}: {error.get('error', 'Bilinmeyen hata')}")
                    error_count += 1
            if failed_group_emails > 3:
                report_lines.append(f"• ... ve {failed_group_emails - 3} diğer hata")
        
        return "\n".join(report_lines)
        
    except Exception as e:
        logger.error(f"Rapor oluşturma hatası: {e}")
        return f"❌ Rapor oluşturma hatası: {str(e)}"
"""   

# utils/reporter.py dosyasında aşağıdaki kısmı güncelleyin:
"""
async def generate_processing_report(result: Dict, report_type: str = "mail") -> str:
    try:
        if not result.get("success", False):
            error_msg = result.get("error", "Bilinmeyen hata")
            if len(error_msg) > 500:
                error_msg = error_msg[:500] + "..."
            return f"❌ İşlem başarısız oldu:\n{error_msg}"
        
        output_files = result.get("output_files", {})
        total_rows = result.get("total_rows", 0)
        matched_rows = result.get("matched_rows", 0)
        unmatched_rows = total_rows - matched_rows
        
        # ✅ DEĞİŞİKLİK: SADECE GRUP MAİLLERİNİ AL
        email_results = result.get("email_results", [])  # Bu zaten sadece grup mailleri
        
        # ✅ SADELEŞTİRME: Başarılı ve başarısız grup mailleri
        successful_group_emails = sum(1 for res in email_results if res.get("success", False))
        failed_group_emails = len(email_results) - successful_group_emails
        
        # ✅ MAIL ve TELEGRAM için AYNI HESAPLAMA (sadece grup mailleri)
        total_successful = successful_group_emails
        total_failed = failed_group_emails
        
        # Diğer mail bilgileri (sadece durum göstermek için)
        bulk_email_sent = result.get("bulk_email_sent", False)
        bulk_email_recipient = result.get("bulk_email_recipient")
        input_email_sent = result.get("input_email_sent", False)
        input_email_recipient = result.get("input_email_recipient")
        
        report_lines = [
            "✅ **EXCEL DOSYA İŞLEME RAPORU_rp**",
            f"⏰ İşlem zamanı: {datetime.now().strftime('%d.%m.%Y %H:%M')}",
            "",
            "📊 *İstatistikler*",
            f"• Toplam satır: {total_rows}",
            f"• Oluşturulan dosya: {len(output_files)}",
            f"• Başarılı mail: {total_successful}",
            f"• Başarısız mail: {total_failed}",
        ]
        
        # 🆕 INPUT MAIL DURUMU (sadece ✅/❌ göstermek için)
        if input_email_sent and input_email_recipient:
            report_lines.append(f"• 📥 Input Maili: ✅")
        elif input_email_recipient:  # Input mail tanımlı ama gönderilememiş
            report_lines.append(f"• 📥 Input Maili: ❌")
        
        # ✅ TELEGRAM İÇİN TOPLU MAIL DURUMU (isteğe bağlı)
        if report_type == "telegram" and bulk_email_recipient:
            if bulk_email_sent:
                report_lines.append(f"• 📧 Toplu Rapor Maili: {bulk_email_recipient} ✅")
            else:
                report_lines.append(f"• 📧 Toplu Rapor Maili: {bulk_email_recipient} ❌")
        
        report_lines.extend([
            "",
            "📁 *Grup Dosyaları:*"
        ])
        
        # ✅ TAM ASYNC: Grup bilgilerini async olarak al
        for group_id, file_info in output_files.items():
            filename = file_info.get("filename", "bilinmeyen")
            row_count = file_info.get("row_count", 0)
            
            group_info = await group_manager.get_group_info(group_id)
            group_name = group_info.get("group_name", group_id)
            
            report_lines.append(f"• {group_name}: {filename} ({row_count} satır)")
        
        # Eşleşmeyen şehirler
        unmatched_cities = result.get("unmatched_cities", [])
        if unmatched_cities:
            report_lines.extend([
                "",
                "⚠️ **Excel Eşleşmeyen Iller:**",
                f"Toplam {len(unmatched_cities)} farklı şehir:"
            ])
            for city in unmatched_cities[:3]:
                report_lines.append(f"• {city}")
            if len(unmatched_cities) > 3:
                report_lines.append(f"• ... ve {len(unmatched_cities) - 3} diğer şehir")
        
        # Mail hataları (sadece grup mail hataları)
        if failed_group_emails > 0:
            report_lines.extend([
                "",
                "❌ **MAIL GÖNDERİM HATALARI:**"
            ])
            error_count = 0
            for error in email_results:
                if not error.get("success", False) and error_count < 3:
                    report_lines.append(f"• {error.get('recipient', 'Bilinmeyen')}: {error.get('error', 'Bilinmeyen hata')}")
                    error_count += 1
            if failed_group_emails > 3:
                report_lines.append(f"• ... ve {failed_group_emails - 3} diğer hata")
        
        return "\n".join(report_lines)
        
    except Exception as e:
        logger.error(f"Rapor oluşturma hatası: {e}")
        return f"❌ Rapor oluşturma hatası: {str(e)}"
"""


async def generate_processing_report(result: Dict, report_type: str = "mail") -> str:
    try:
        # ---------------------------------------------------------------
        # 0) GENEL HATA KONTROLÜ
        # ---------------------------------------------------------------
        if not result.get("success", False):
            error_msg = result.get("error", "Bilinmeyen hata")
            if len(error_msg) > 500:
                error_msg = error_msg[:500] + "..."
            return f"❌ İşlem başarısız oldu:\n{error_msg}"

        # ---------------------------------------------------------------
        # 1) VERİLERİ GÜVENLİ AL
        # ---------------------------------------------------------------
        output_files = result.get("output_files", {})
        total_rows = result.get("total_rows", 0)
        matched_rows = result.get("matched_rows", 0)
        unmatched_rows = total_rows - matched_rows

        # ---------------------------------------------------------------
        # 2) GRUP MAIL SONUÇLARI – DAİMA LİSTEYE ÇEVİR
        # process_excel_task şu formatta gönderiyor:
        # "email_results": group_results  (group_results bir DICT)
        # Bu nedenle DICT → LIST dönüşümü gerekli
        # ---------------------------------------------------------------
        email_results_raw = result.get("email_results", {})
        if isinstance(email_results_raw, dict):
            email_results = list(email_results_raw.values())
        else:
            email_results = email_results_raw  # Zaten list ise dokunma

        # Başarılı / başarısız grup mail sayısı
        successful_group_emails = sum(1 for res in email_results if res.get("success"))
        failed_group_emails = len(email_results) - successful_group_emails

        # ---------------------------------------------------------------
        # 3) INPUT & BULK MAIL DURUMUNU AL (dict → success extract)
        # ---------------------------------------------------------------
        input_mail_raw = result.get("input_email_sent", {})
        input_email_sent = (
            input_mail_raw.get("success") if isinstance(input_mail_raw, dict)
            else bool(input_mail_raw)
        )
        input_email_recipient = result.get("input_email_recipient")

        bulk_mail_raw = result.get("bulk_email_sent", {})
        bulk_email_sent = (
            bulk_mail_raw.get("success") if isinstance(bulk_mail_raw, dict)
            else bool(bulk_mail_raw)
        )
        bulk_email_recipient = result.get("bulk_email_recipient")

        # ---------------------------------------------------------------
        # 4) RAPOR METNİ
        # ---------------------------------------------------------------
        report_lines = [
            "✅ **EXCEL İŞLEM RAPORU**",
            f"⏰ İşlem zamanı: {datetime.now().strftime('%d.%m.%Y %H:%M')}",
            "",
            "📊 *İstatistikler*",
            f"• Toplam satır: {total_rows}",
            f"• Oluşturulan grup dosyası: {len(output_files)}",
            f"• Başarılı grup maili: {successful_group_emails}",
            f"• Başarısız grup maili: {failed_group_emails}",
        ]

        # ---------------------------------------------------------------
        # 5) INPUT MAIL DURUMU
        # ---------------------------------------------------------------
        if input_email_recipient:
            emoji = "✅" if input_email_sent else "❌"
            report_lines.append(f"• 📥 Input Maili: {emoji}")

        # ---------------------------------------------------------------
        # 6) BULK (KİŞİSEL) MAIL DURUMU – sadece telegramda göster
        # ---------------------------------------------------------------
        if report_type == "telegram" and bulk_email_recipient:
            emoji = "✅" if bulk_email_sent else "❌"
            report_lines.append(f"• 📧 Kişisel Rapor Maili: {bulk_email_recipient} {emoji}")

        # ---------------------------------------------------------------
        # 7) GRUP DOSYALARI
        # ---------------------------------------------------------------
        report_lines.extend(["", "📁 *Grup Dosyaları:*"])

        for group_id, file_info in output_files.items():
            filename = file_info.get("filename", "bilinmeyen")
            row_count = file_info.get("row_count", 0)

            # Grup adı bilgisi async alınır
            group_info = await group_manager.get_group_info(group_id)
            group_name = group_info.get("group_name", group_id)

            report_lines.append(f"• {group_name}: {filename} ({row_count} satır)")

        # ---------------------------------------------------------------
        # 8) EŞLEŞMEYEN ŞEHİRLER
        # ---------------------------------------------------------------
        unmatched_cities = result.get("unmatched_cities", [])
        if unmatched_cities:
            report_lines.extend([
                "",
                "⚠️ **Excel'de Bulunamayan Şehirler:**",
                f"Toplam {len(unmatched_cities)} şehir:"
            ])
            for city in unmatched_cities[:3]:
                report_lines.append(f"• {city}")
            if len(unmatched_cities) > 3:
                report_lines.append(f"• ... ve {len(unmatched_cities) - 3} diğer şehir")

        # ---------------------------------------------------------------
        # 9) GRUP MAIL HATALARI
        # ---------------------------------------------------------------
        if failed_group_emails > 0:
            report_lines.extend(["", "❌ **Grup Maili Hataları:**"])

            shown = 0
            for res in email_results:
                if not res.get("success") and shown < 3:
                    report_lines.append(
                        f"• {res.get('recipient', 'Bilinmeyen')}: "
                        f"{res.get('error', 'Hata detayı yok')}"
                    )
                    shown += 1

            if failed_group_emails > 3:
                report_lines.append(f"• ... ve {failed_group_emails - 3} diğer hata")

        # ---------------------------------------------------------------
        # 10) SONUÇ
        # ---------------------------------------------------------------
        return "\n".join(report_lines)

    except Exception as e:
        logger.error(f"Rapor oluşturma hatası: {e}", exc_info=True)
        return f"❌ Rapor oluşturma hatası: {str(e)}"







async def generate_email_report(email_results: List[Dict]) -> str:
    """✅ Email gönderim raporu oluşturur - TAM ASYNC"""
    try:
        successful = sum(1 for res in email_results if res.get("success", False))
        failed = len(email_results) - successful
        
        report = [
            f"📧 **EMAIL RAPORU_1**",
            f"⏰ İşlem zamanı: {datetime.now().strftime('%d.%m.%Y %H:%M')}",
            f"✅ Başarılı: {successful}",
            f"❌ Başarısız: {failed}",
            ""
        ]
        
        #  hata listesi      
        if failed > 0:
            report.append("**Hatalar:**")
            # Sadece başarısız sonuçları al ve ilk 7 tanesini listele
            failed_results = [r for r in email_results if not r.get("success", False)][:7]
            for i, fail in enumerate(failed_results, 1):
                report.append(
                    f"{i}. {fail.get('recipient', 'Bilinmeyen')}: "
                    f"{fail.get('error', 'Bilinmeyen hata')}"
                )



                
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