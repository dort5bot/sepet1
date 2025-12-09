# utils/mailer.py
"""
Mailer V5 
PRO - Stabilite Sürümü
- Persistent SMTP connection (reconnect on failure)
- Async queue + semaphore limiting concurrency
- Unified send_email API with helpers that preserve Version 1 functionality
- Robust retry with exponential backoff
- Safe attachment handling (reads attachments per-email, no global preloading)
- ZIP helper for input+output bundles

Serial-input mail → parallel-group mail → serial-bulk mail yönetimi
MTP Hatası Olursa Kesin Olarak Ne Olur?
Zaman aşımı 20 saniyede devreye girer
Mevcut bağlantı kapanır
Tüm SMTP portlarına yeniden bağlanmayı dener
Send yeniden denenir
Exponential backoff ile maksimum 3 send denemesi yapılır
Hâlâ başarısızsa:
_send_with_controls() False döner
send_batch() iş kaydını success=False + error ile işaretler
Sistem kilitlenmez, takılmaz, sonsuz beklemez

Notes:
- Configure config.email.SMTP_SERVER, SMTP_PORTS (list), SMTP_USERNAME, SMTP_PASSWORD,
  PERSONAL_EMAIL, INPUT_EMAIL in your config module.
- Tweak MAX_PARALLEL_SEND default if you want more/less concurrency.
- This implementation uses a single persistent connection and a semaphore to limit parallel
  send attempts. If you want connection pooling, it can be extended.
"""
# utils/mailer.py
import asyncio
import logging
import ssl
import mimetypes
import tempfile
import zipfile
from pathlib import Path
from datetime import datetime
from typing import List, Optional, Dict, Any
from email.message import EmailMessage

import aiosmtplib

from config import config

logger = logging.getLogger(__name__)

# mailler kitleme
_default_mailer: Optional["MailerV2"] = None
_default_mailer_lock = asyncio.Lock()

async def get_default_mailer() -> "MailerV2":
    global _default_mailer
    async with _default_mailer_lock:
        if _default_mailer is None:
            _default_mailer = MailerV2()
            await _default_mailer.start()
        return _default_mailer
        
class SMTPConnectionManager:
    """Manages a single persistent SMTP connection with reconnection logic."""

    def __init__(self, loop: Optional[asyncio.AbstractEventLoop] = None):
        self.loop = loop or asyncio.get_event_loop()
        self._client: Optional[aiosmtplib.SMTP] = None
        self._lock = asyncio.Lock()
        self._connected_port: Optional[int] = None

    async def connect(self, ports: List[int], max_retries: int = 2) -> bool:
        """Try to connect using provided ports. Keeps one persistent client."""
        async with self._lock:
            if self._client and self._client.is_connected:
                logger.debug("SMTP already connected")
                return True

            smtp_server = getattr(config.email, 'SMTP_SERVER', None)
            username = getattr(config.email, 'SMTP_USERNAME', None)
            password = getattr(config.email, 'SMTP_PASSWORD', None)

            if not smtp_server or not username or not password:
                logger.error("SMTP configuration missing (SERVER/USERNAME/PASSWORD)")
                return False

            ssl_context = ssl.create_default_context()

            for port in ports:
                # Determine TLS mode
                if port == 465:
                    use_tls = True
                    start_tls = False
                elif port == 587:
                    use_tls = False
                    start_tls = True
                else:
                    use_tls = False
                    start_tls = True
                    logger.warning(f"Unknown SMTP port {port} specified; will attempt STARTTLS")

                for attempt in range(1, max_retries + 2):
                    try:
                        client = aiosmtplib.SMTP(
                            hostname=smtp_server,
                            port=port,
                            start_tls=start_tls,
                            use_tls=use_tls,
                            tls_context=ssl_context,
                        )
                        #await client.connect()
                        await asyncio.wait_for(client.connect(), timeout=20)


                        # If STARTTLS, library usually handles the start; ensure login
                        #await client.login(username, password)
                        await asyncio.wait_for(client.login(username, password), timeout=20)


                        # assign persistent client
                        self._client = client
                        self._connected_port = port
                        logger.info(f"Connected to SMTP {smtp_server}:{port}")
                        return True

                    except Exception as e:
                        logger.error(f"SMTP connect failed {smtp_server}:{port} attempt {attempt}: {e}")
                        if attempt < (max_retries + 1):
                            wait = 2 ** (attempt - 1)
                            logger.debug(f"Waiting {wait}s before retrying connection")
                            await asyncio.sleep(wait)
                        else:
                            logger.error(f"All attempts failed for port {port}")

            logger.error("Failed to connect to any SMTP port")
            return False

    async def disconnect(self) -> None:
        async with self._lock:
            if self._client:
                try:
                    await self._client.quit()
                except Exception:
                    try:
                        await self._client.close()
                    except Exception:
                        pass
                finally:
                    self._client = None
                    self._connected_port = None
                    logger.info("SMTP client disconnected")

    # timeout eklendi: sistem tamamen kilitlenmez
    async def send_message(self, message: EmailMessage) -> None:
        """Sends message using persistent client. Will try to reconnect if needed."""
        # We'll attempt a few reconnects if send fails
        ports = getattr(config.email, 'SMTP_PORTS', [465, 587])
        max_retries = getattr(config.email, 'SMTP_MAX_RETRIES', 2)

        # Acquire lock to avoid concurrent connects interfering with client state
        async with self._lock:
            # Ensure connected
            if not (self._client and getattr(self._client, 'is_connected', False)):
                connected = await self.connect(ports, max_retries=max_retries)
                if not connected:
                    raise RuntimeError("Unable to connect to SMTP server")

            try:
                # timeout eklenerek takılma çözülür, oto kapanır
                # await self._client.send_message(message)
                # SMTP sunucu yanıt vermezse timeout→ 20 sn sonra otomatik TimeoutError
                await asyncio.wait_for(
                    self._client.send_message(message),
                    timeout=20
                )
                            
            except Exception as send_exc:
                logger.warning(f"Send failed on existing connection: {send_exc}; attempting reconnect and retry")
                # try reconnect and resend
                try:
                    await self.disconnect()
                except Exception:
                    pass

                connected = await self.connect(ports, max_retries=max_retries)
                if not connected:
                    raise RuntimeError("Unable to reconnect to SMTP server after send failure")

                # try one more time
                try:
                    #await self._client.send_message(message)
                    await asyncio.wait_for(
                        self._client.send_message(message),
                        timeout=20
                    )
                            
                except Exception as e:
                    logger.error(f"Retry send after reconnect failed: {e}")
                    raise


class MailerV2:
    """High-level mailer with stability features."""

    DEFAULT_MAX_PARALLEL = 3

    def __init__(self, max_parallel: int = None):
        self.max_parallel = max_parallel or self.DEFAULT_MAX_PARALLEL
        self._semaphore = asyncio.Semaphore(self.max_parallel)
        self._conn_mgr = SMTPConnectionManager()
        self._started = False

    async def start(self) -> bool:
        """Start the mailer (establish persistent SMTP connection)."""
        if self._started:
            return True
        ports = getattr(config.email, 'SMTP_PORTS', [465, 587])
        ok = await self._conn_mgr.connect(ports)
        if ok:
            self._started = True
        return ok

    async def stop(self) -> None:
        await self._conn_mgr.disconnect()
        self._started = False

    # mailgönderim sıralaması
    # 1) group mails first (parallel)
    # 2) input mails (serial) AFTER group
    # - Grup mail sonuçları temiz kapandıktan sonra başlar
    # 3) bulk/rapor (serial)

    # grup maİllerİ gönderimi: 1/2 - sadece görev yaratır
    # Gönderim sırasını belirlenir:

    # -1-
    """async def send_batch(self, jobs: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        #Correct mail order:
        #    1) group mails (parallel)
        #    2) input mail (serial, AFTER groups)
        #    3) bulk mail / rapor (serial)

        results = []

        # ensure single connect
        if not self._started:
            connected = await self.start()
            if not connected:
                for i, job in enumerate(jobs):
                    results.append({"job_index": i, "success": False, "error": "Mailer cannot connect"})
                return results

        # -------------------------------------------------------------
        # 1) GROUP MAILS (PARALLEL)
        # -------------------------------------------------------------
        group_tasks = []
        group_indices = []

        for i, job in enumerate(jobs):
            if job.get("type") == "group":
                coro = self.send_email_with_attachment(
                    job["to"], job["subject"], job["body"], job["attachments"][0]
                )
                task = asyncio.create_task(_wrap_job(coro, i))
                group_tasks.append(task)
                group_indices.append(i)

        if group_tasks:
            try:
                group_results = await asyncio.wait_for(
                    asyncio.gather(*group_tasks, return_exceptions=True),
                    timeout=360  # 6 dakika
                )
            except asyncio.TimeoutError:
                logger.error("GLOBAL TIMEOUT: group mail tasks exceeded 360 seconds")
                for idx in group_indices:
                    results.append({
                        "job_index": idx,
                        "success": False,
                        "error": "Global timeout (6 min) while sending group mails"
                    })
                group_results = []

            for res in group_results:
                results.append(res)

        # -------------------------------------------------------------
        # 2) INPUT MAIL (SERIAL — GROUP BİTTİKTEN SONRA)
        # -------------------------------------------------------------
        for i, job in enumerate(jobs):
            if job.get("type") == "input":
                try:
                    ok = await self.send_email_with_attachment(
                        job["to"], job["subject"], job["body"], job["attachments"][0]
                    )
                    results.append({"job_index": i, "success": ok})
                except Exception as e:
                    results.append({"job_index": i, "success": False, "error": str(e)})

        # -------------------------------------------------------------
        # 3) BULK / RAPOR (SERIAL)
        # -------------------------------------------------------------
        for i, job in enumerate(jobs):
            if job.get("type") == "bulk":
                try:
                    input_path = job["meta"].get("input_path")
                    output_files = job["meta"].get("output_files")

                    zip_path = await self._create_bulk_zip(input_path, output_files)
                    if not zip_path:
                        results.append({"job_index": i, "success": False, "error": "ZIP creation failed"})
                        continue

                    ok = await self.send_email_with_attachment(
                        job["to"], job["subject"], job["body"], zip_path
                    )
                    results.append({"job_index": i, "success": ok})

                except Exception as e:
                    results.append({"job_index": i, "success": False, "error": str(e)})

        return results
    """

    # -2-
    # -2- results listesinin jobs index sırasına göre tutulması, sıra takibi
    async def send_batch(self, jobs: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        #Correct mail order:
        #    1) group mails (parallel)
        #    2) input mail (serial, AFTER groups)
        #    3) bulk mail / rapor (serial)
        #results listesi artık JOB SIRASI ile birebir aynı index'te tutulur.

        # results dizisinin uzunluğu kadar sabit liste oluştur
        results = [None] * len(jobs)

        # ensure single connect
        if not self._started:
            connected = await self.start()
            if not connected:
                # tüm job’ları mailer bağlantı hatası olarak işaretle
                for i in range(len(jobs)):
                    results[i] = {
                        "job_index": i,
                        "success": False,
                        "error": "Mailer cannot connect"
                    }
                return results

        # -------------------------------------------------------------
        # 1) GROUP MAILS (PARALLEL)
        # -------------------------------------------------------------
        group_tasks = {}   # task → job_index

        for i, job in enumerate(jobs):
            if job.get("type") == "group":
                coro = self.send_email_with_attachment(
                    job["to"], job["subject"], job["body"], job["attachments"][0]
                )
                task = asyncio.create_task(coro)
                group_tasks[task] = i

        if group_tasks:
            try:
                done = await asyncio.wait_for(
                    asyncio.gather(*group_tasks.keys(), return_exceptions=True),
                    timeout=360  # 6 dakika
                )

                # sonuçları kendi indexlerine yaz
                for task, outcome in zip(group_tasks.keys(), done):
                    job_index = group_tasks[task]

                    if isinstance(outcome, Exception):
                        results[job_index] = {
                            "job_index": job_index,
                            "success": False,
                            "error": str(outcome)
                        }
                    else:
                        results[job_index] = {
                            "job_index": job_index,
                            "success": bool(outcome)
                        }

            except asyncio.TimeoutError:
                logger.error("GLOBAL TIMEOUT: group mail tasks exceeded 360 seconds")

                for task, job_index in group_tasks.items():
                    results[job_index] = {
                        "job_index": job_index,
                        "success": False,
                        "error": "Global timeout (6 min) while sending group mails"
                    }

        # -------------------------------------------------------------
        # 2) INPUT MAIL (SERIAL)
        # -------------------------------------------------------------
        for i, job in enumerate(jobs):
            if job.get("type") == "input":
                try:
                    ok = await self.send_email_with_attachment(
                        job["to"], job["subject"], job["body"], job["attachments"][0]
                    )
                    results[i] = {
                        "job_index": i,
                        "success": ok
                    }
                except Exception as e:
                    results[i] = {
                        "job_index": i,
                        "success": False,
                        "error": str(e)
                    }

        # -------------------------------------------------------------
        # 3) BULK / RAPOR MAIL (SERIAL)
        # -------------------------------------------------------------
        for i, job in enumerate(jobs):
            if job.get("type") == "bulk":
                try:
                    input_path = job["meta"].get("input_path")
                    output_files = job["meta"].get("output_files")

                    zip_path = await self._create_bulk_zip(input_path, output_files)
                    if not zip_path:
                        results[i] = {
                            "job_index": i,
                            "success": False,
                            "error": "ZIP creation failed"
                        }
                        continue

                    ok = await self.send_email_with_attachment(
                        job["to"], job["subject"], job["body"], zip_path
                    )
                    results[i] = {
                        "job_index": i,
                        "success": ok
                    }

                except Exception as e:
                    results[i] = {
                        "job_index": i,
                        "success": False,
                        "error": str(e)
                    }

        return results



    # ------------------------------------------------------------
    # low-level send wrapper with semaphore and retry/backoff
    # grup maİllerİ gönderimi: 2/2 - görevlerin gönderimini yapar
    # ------------------------------------------------------------
    async def _send_with_controls(self, message: EmailMessage, recipients: List[str], max_retries: int = 2) -> bool:
        # semaphore limits parallel sends to avoid overwhelming SMTP
        async with self._semaphore:
            # exponential backoff for send-level retries
            for attempt in range(1, max_retries + 2):
                try:
                    # ensure started
                    if not self._started:
                        connected = await self.start()
                        if not connected:
                            raise RuntimeError("Mailer not started and cannot connect")

                    await self._conn_mgr.send_message(message)
                    logger.info(f"Mail sent to {recipients}")
                    return True
                except Exception as e:
                    logger.error(f"Send attempt {attempt} failed for {recipients}: {e}")
                    if attempt < (max_retries + 1):
                        wait = 2 ** (attempt - 1)
                        logger.debug(f"Waiting {wait}s before next send attempt")
                        await asyncio.sleep(wait)
                    else:
                        logger.error(f"All send attempts failed for {recipients}")
                        return False

    # helper to attach single file with correct MIME
    def _attach_file_to_message(self, msg: EmailMessage, path: Path) -> None:
        ctype, encoding = mimetypes.guess_type(str(path))
        if ctype is None:
            ctype = 'application/octet-stream'
        maintype, subtype = ctype.split('/', 1)

        # read file in memory per file (minimizes simultaneous memory usage)
        # we avoid preloading many files at once by doing this per message
        with path.open('rb') as f:
            data = f.read()

        msg.add_attachment(data, maintype=maintype, subtype=subtype, filename=path.name)
        logger.debug(f"Attached {path} ({ctype})")

    # Public API - Version 1 compatibility functions preserved & improved
    async def send_simple_email(self, to_emails: List[str], subject: str, body: str, max_retries: int = 2) -> bool:
        if not to_emails or not any(to_emails):
            logger.warning("No recipients for simple email")
            return False

        msg = EmailMessage()
        msg['From'] = config.email.SMTP_USERNAME
        msg['To'] = ', '.join(to_emails)
        msg['Subject'] = subject
        msg.set_content(body)

        return await self._send_with_controls(msg, to_emails, max_retries=max_retries)

    async def send_email_with_attachment(self, to_emails: List[str], subject: str, body: str, attachment_path: Path, max_retries: int = 2) -> bool:
        if not to_emails or not any(to_emails):
            logger.warning("No recipients")
            return False
        if not attachment_path or not attachment_path.exists():
            logger.error(f"Attachment not found: {attachment_path}")
            return False

        msg = EmailMessage()
        msg['From'] = config.email.SMTP_USERNAME
        msg['To'] = ', '.join(to_emails)
        msg['Subject'] = subject
        msg.set_content(body)

        # attach single file (reads file once)
        self._attach_file_to_message(msg, attachment_path)

        return await self._send_with_controls(msg, to_emails, max_retries=max_retries)

    async def send_email_with_multiple_attachments(self, to_emails: List[str], subject: str, body: str, attachment_paths: List[Path], max_retries: int = 2) -> bool:
        if not to_emails or not any(to_emails):
            logger.warning("No recipients")
            return False
        if not attachment_paths:
            logger.warning("No attachment paths provided")
            return False

        msg = EmailMessage()
        msg['From'] = config.email.SMTP_USERNAME
        msg['To'] = ', '.join(to_emails)
        msg['Subject'] = subject
        msg.set_content(body)

        attached = 0
        for p in attachment_paths:
            if p and p.exists():
                self._attach_file_to_message(msg, p)
                attached += 1
            else:
                logger.warning(f"Attachment missing: {p}")

        if attached == 0:
            logger.error("No valid attachments to send")
            return False

        return await self._send_with_controls(msg, to_emails, max_retries=max_retries)

    async def _create_bulk_zip(self, input_path: Optional[Path], output_files: Dict[str, Dict[str, Any]]) -> Optional[Path]:
        try:
            zip_path = Path(tempfile.gettempdir()) / f"Rapor_{datetime.now().strftime('%Y%m%d_%H%M%S')}.zip"
            with zipfile.ZipFile(zip_path, 'w', zipfile.ZIP_DEFLATED) as zf:
                if input_path and input_path.exists():
                    zf.write(input_path, f"input/{input_path.name}")
                    logger.debug(f"ZIP includes input: {input_path.name}")

                for group_id, file_info in (output_files or {}).items():
                    p = file_info.get('path')
                    filename = file_info.get('filename') or (p.name if p else None)
                    if p and p.exists():
                        zf.write(p, filename)
                        logger.debug(f"ZIP includes: {filename}")

            logger.info(f"ZIP created at {zip_path}")
            return zip_path
        except Exception as e:
            logger.error(f"ZIP creation error: {e}")
            return None

    async def send_automatic_bulk_email(self, input_path: Path, output_files: Dict[str, Dict[str, Any]], processing_report: str = "", max_retries: int = 2) -> bool:
        personal = getattr(config.email, 'PERSONAL_EMAIL', None)
        if not personal:
            logger.error("PERSONAL_EMAIL not configured")
            return False

        zip_path = await self._create_bulk_zip(input_path, output_files)
        if not zip_path:
            return False

        subject = f"📊 TelData Raporu - {input_path.name}"
        
        # Raporu mail gövdesine ekle
        body = (
            "Merhaba,\n\nTelefon dataları işleme sonucu oluşan tüm dosyalar ektedir.\n\n"
        )
        
        if processing_report:
            body += "İŞLEM RAPORU:\n" + processing_report + "\n\n"
        
        body += "İyi çalışmalar,\nData_listesi_Hıdır"

        try:
            success = await self.send_email_with_attachment([personal], subject, body, zip_path, max_retries=max_retries)
            return success
        finally:
            try:
                if zip_path.exists():
                    zip_path.unlink()
                    logger.debug(f"Temporary zip removed: {zip_path}")
            except Exception as e:
                logger.warning(f"Failed to remove temporary zip: {e}")
                

    async def send_input_only_email(self, input_path: Path, max_retries: int = 2) -> bool:
        input_email = getattr(config.email, 'INPUT_EMAIL', None)
        if not input_email:
            logger.info("INPUT_EMAIL not configured, skipping input send")
            return False
        if not input_path or not input_path.exists():
            logger.error(f"Input file missing: {input_path}")
            return False

        subject = f"📥 TelPex Input excel - {input_path.name}"
        body = (f"Merhaba,\n\nTelefon data dosyası ektedir.\nDosya: {input_path.name}\n\nİyi çalışmalar,\nData_listesi_Hıdır")

        return await self.send_email_with_attachment([input_email], subject, body, input_path, max_retries=max_retries)

# helper outside class
async def _wrap_job(coro, job_index):
    try:
        ok = await coro
        return {"job_index": job_index, "success": bool(ok)}
    except Exception as e:
        return {"job_index": job_index, "success": False, "error": str(e)}
        
# End of Mailer V2 PRO
