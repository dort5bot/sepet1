#Mail Gönderici (utils/mailer.py)
# Mailer Kodunu Güncelleyin (Detaylı Loglama):
"""
versin: 27/11/2025 18:28
Gönderen adrese görünen isim eklendi: 
Data_listesi_Hıdır <user@domain.com>
Mail header’larına X-Priority ve X-Mailer eklendi
Hem plain text hem HTML body eklendi (modern e-posta uyumu için)
Gmail spam'e düşürüyorsa, farklı bir SMTP servisi deneyin:
Yandex Mail (smtp.yandex.com)
Outlook/Hotmail (smtp-mail.outlook.com)

Amaç: E-posta gönderme işlemlerini yönetir

İşlevler:
send_email_with_attachment(): Tekil e-posta gönderimi
send_automatic_bulk_email(): Toplu e-posta gönderimi
_create_bulk_zip(): ZIP dosyası oluşturma
Özellik: SMTP bağlantısı, SSL/TLS yönetimi, ek dosya işleme
"""

# utils/mailer.py - DÜZELTİLMİŞ VERSİYON

import logging 
import aiosmtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.application import MIMEApplication
from pathlib import Path
from datetime import datetime
import tempfile
import zipfile
from config import config
from utils.logger import logger
import ssl

# Logger tanımla
logger = logging.getLogger(__name__) 


async def send_email_with_attachment(
    to_emails: list,
    subject: str,
    body: str,
    attachment_path: Path,
    max_retries: int = 2
) -> bool:
    """E-posta gönderir (ekli dosya ile) - DETAYLI LOGLAMALI"""
    
    # DEBUG: Başlangıç bilgileri
    #logger.info(f"🔍 DEBUG - Mail gönderimi başlıyor:")
    #logger.info(f"🔍 DEBUG - Alıcılar: {to_emails}")
    #logger.info(f"🔍 DEBUG - Konu: {subject}")
    #logger.info(f"🔍 DEBUG - SMTP Server: {config.email.SMTP_SERVER}")  # DÜZELTME: config.email.SMTP_SERVER
    #logger.info(f"🔍 DEBUG - SMTP User: {config.email.SMTP_USERNAME}")  # DÜZELTME: config.email.SMTP_USERNAME
    #logger.info(f"🔍 DEBUG - SMTP Ports: {config.email.SMTP_PORTS}")    # DÜZELTME: config.email.SMTP_PORTS
    #logger.info(f"🔍 DEBUG - Attachment: {attachment_path}")
    #logger.info(f"🔍 DEBUG - Attachment exists: {attachment_path.exists()}")
    
    if not to_emails or not any(to_emails):
        logger.warning("Alıcı email adresi yok")
        return False
    
    # SSL context oluştur
    ssl_context = ssl.create_default_context()
    
    successful = False
    
    # DEBUG: Port listesi
    logger.info(f"🔍 DEBUG - Denenecek portlar: {config.email.SMTP_PORTS}")  # DÜZELTME
    
    for port in config.email.SMTP_PORTS:  # DÜZELTME: config.email.SMTP_PORTS
        for attempt in range(max_retries + 1):
            try:
                logger.info(f"📧 Mail gönderimi deneniyor: {to_emails}, Port: {port}, Deneme: {attempt + 1}")
                
                message = MIMEMultipart()
                message["From"] = config.email.SMTP_USERNAME  # DÜZELTME
                message["To"] = ", ".join(to_emails)
                message["Subject"] = subject
                
                # Mesaj gövdesi
                message.attach(MIMEText(body, "plain", "utf-8"))
                
                # Dosya eki
                if attachment_path.exists():
                    file_size = attachment_path.stat().st_size / 1024  # KB
                    logger.info(f"📎 Eklenecek dosya: {attachment_path.name} ({file_size:.1f} KB)")
                    
                    with open(attachment_path, "rb") as f:
                        attachment = MIMEApplication(f.read(), _subtype="xlsx")
                        attachment.add_header(
                            "Content-Disposition",
                            "attachment",
                            filename=attachment_path.name
                        )
                        message.attach(attachment)
                else:
                    logger.warning(f"❌ Eklenecek dosya bulunamadı: {attachment_path}")
                    return False
                
                # PORT'A GÖRE BAĞLANTI AYARLARI
                use_tls = port == 465  # 465 için SSL, 587 için STARTTLS
                
                logger.info(f"🔌 SMTP bağlantısı: {config.email.SMTP_SERVER}:{port} (TLS: {use_tls})")  # DÜZELTME

          
                if port == 465: # (SSL/TLS)
                    # SSL (doğrudan TLS)
                    async with aiosmtplib.SMTP(
                        hostname=config.email.SMTP_SERVER,
                        port=465,
                        use_tls=True,
                        tls_context=ssl_context
                    ) as server:
                        await server.login(config.email.SMTP_USERNAME, config.email.SMTP_PASSWORD)
                        await server.send_message(message)

                else:  # 587  (STARTTLS)
                    async with aiosmtplib.SMTP(
                        hostname=config.email.SMTP_SERVER,
                        port=587,
                        start_tls=True,     # ✔ DOĞRUSU BU
                        use_tls=False,      # ✔ BURASI FALSE KALMALI
                        tls_context=ssl_context
                    ) as server:
                        await server.login(config.email.SMTP_USERNAME, config.email.SMTP_PASSWORD)
                        await server.send_message(message)
               
                
                
                
                logger.info(f"✅ Mail BAŞARIYLA gönderildi: {to_emails}")
                successful = True
                break  # Başarılı oldu, diğer portları deneme
                
            except Exception as e:
                error_msg = str(e)
                logger.error(f"❌ Mail gönderme hatası (Port: {port}, Deneme: {attempt + 1}): {error_msg}")
                
                # Son denemede logla
                if attempt == max_retries:
                    logger.error(f"❌ Port {port} için tüm denemeler başarısız")
                
                # Bekle ve tekrar dene
                if attempt < max_retries:
                    wait_time = 2 ** attempt
                    import asyncio
                    await asyncio.sleep(wait_time)
        
        if successful:
            break  # Başarılı oldu, diğer portları deneme
    
    if not successful:
        logger.error(f"❌❌❌ TÜM MAIL GÖNDERME DENEMELERİ BAŞARISIZ: {to_emails}")
    
    return successful


# PERSONAL_EMAIL > input+outpu =zip > gider > env de tanımlı = ersin >PERSONAL_EMAIL 
async def send_automatic_bulk_email(input_path: Path, output_files: dict) -> bool:
    """Otomatik toplu mail gönderimi"""
    try:
        if not config.email.PERSONAL_EMAIL:
            logger.error("PERSONAL_EMAIL tanımlı değil")
            return False

        # ZIP oluştur
        zip_path = await _create_bulk_zip(input_path, output_files)
        if not zip_path:
            return False

        subject = "📊 Telefon data  Raporu "
        body = (
            "Merhaba,\n\n"
            "Telefon dataları işleme sonucu oluşan tüm dosyalar ektedir.\n\n"
            "Gelen dosya ve grup dosyaları \n"
            "İyi çalışmalar,\nData_listesi_Hıdır"
        )

        success = await send_email_with_attachment(
            [config.email.PERSONAL_EMAIL],
            subject,
            body,
            zip_path
        )

        # Temizlik
        if zip_path.exists():
            zip_path.unlink()

        return success

    except Exception as e:
        logger.error(f"Toplu mail hatası: {e}")
        return False


async def _create_bulk_zip(input_path: Path, output_files: dict) -> Path:
    """Toplu mail için ZIP oluştur"""
    try:
        zip_path = Path(tempfile.gettempdir()) / f"Rapor_{datetime.now().strftime('%m%d_%H%M')}.zip"
        
        with zipfile.ZipFile(zip_path, 'w', zipfile.ZIP_DEFLATED) as zipf:
            # Input dosyasını ekle
            if input_path.exists():
                zipf.write(input_path, f"input/{input_path.name}")
            
            # Output dosyalarını ekle
            for group_id, file_info in output_files.items():
                if file_info["path"].exists():
                    zipf.write(file_info["path"], file_info['filename'])
        
        return zip_path
    except Exception as e:
        logger.error(f"ZIP oluşturma hatası: {e}")
        return None
        