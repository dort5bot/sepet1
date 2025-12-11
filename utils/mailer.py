# utils/mailer6.py
"""
Mailer V6 - Hybrid (Connection Pool + Group Parallel / Input & Bulk Serial)
- Persistent connection pool (configurable size)
- Group mails: parallel with semaphore + per-domain concurrency limits
- Input & Bulk mails: sent serially after groups
- Per-task timeouts (no global blocking wait)
- Circuit breaker for repeated transient failures
- Attachment size checks (option to upload large files elsewhere)
- Healthcheck helper methods
- Keeps public API names compatible: get_default_mailer, MailerV2, send_batch, send_email_with_attachment, ...
"""

# utils/mailer.py
import asyncio
import logging
import ssl
import mimetypes
import tempfile
import zipfile
from pathlib import Path
from datetime import datetime, timedelta
from typing import List, Optional, Dict, Any, Tuple
from email.message import EmailMessage

import aiosmtplib

from config import config  # expects config.email.* attributes

logger = logging.getLogger(__name__)

# -------------------------
# Defaults (can be overridden via config.email)
# -------------------------
DEFAULTS = {
    "SMTP_PORTS": [465, 587],
    "MAX_PARALLEL_GROUP": 3,
    "CONNECTION_POOL_SIZE": 2,
    "PER_DOMAIN_CONCURRENCY": 1,
    "CONNECT_TIMEOUT": 5,
    "LOGIN_TIMEOUT": 5,
    "SEND_TIMEOUT": 30,
    "MAX_SEND_RETRIES": 2,
    "CONNECT_RETRIES": 2,
    "MAX_ATTACHMENT_SIZE_MB": 15,
    "CIRCUIT_BREAKER_FAILURES": 20,
    "CIRCUIT_BREAKER_WINDOW_SEC": 300,
    "CIRCUIT_BREAKER_COOLDOWN_SEC": 300,
}

# -------------------------
# Singleton default mailer
# -------------------------
_default_mailer: Optional["MailerV2"] = None
_default_mailer_lock = asyncio.Lock()


async def get_default_mailer() -> "MailerV2":
    global _default_mailer
    async with _default_mailer_lock:
        if _default_mailer is None:
            _default_mailer = MailerV2()
            await _default_mailer.start()
        return _default_mailer


# -------------------------
# Helper utilities
# -------------------------
def _get_conf(name: str):
    return getattr(config.email, name, DEFAULTS.get(name))


def _bytes_to_mb(n: int) -> float:
    return n / (1024 * 1024)


# -------------------------
# Circuit Breaker
# -------------------------
class SimpleCircuitBreaker:
    """
    Tracks recent failures in a sliding time window.
    If failures exceed threshold, circuit opens for cooldown period.
    """

    def __init__(self,
                 failure_threshold: int = DEFAULTS["CIRCUIT_BREAKER_FAILURES"],
                 window_sec: int = DEFAULTS["CIRCUIT_BREAKER_WINDOW_SEC"],
                 cooldown_sec: int = DEFAULTS["CIRCUIT_BREAKER_COOLDOWN_SEC"]):
        self.failure_threshold = failure_threshold
        self.window = timedelta(seconds=window_sec)
        self.cooldown = timedelta(seconds=cooldown_sec)
        self.failures: List[datetime] = []
        self.open_until: Optional[datetime] = None
        self._lock = asyncio.Lock()

    async def record_failure(self) -> None:
        async with self._lock:
            now = datetime.utcnow()
            self.failures.append(now)
            # cleanup
            cutoff = now - self.window
            self.failures = [t for t in self.failures if t >= cutoff]
            if len(self.failures) >= self.failure_threshold:
                self.open_until = now + self.cooldown
                logger.warning(f"Circuit opened until {self.open_until.isoformat()} due to repeated failures")

    async def record_success(self) -> None:
        async with self._lock:
            # on success, we can trim failures list a bit
            now = datetime.utcnow()
            cutoff = now - self.window
            self.failures = [t for t in self.failures if t >= cutoff]
            # optionally reduce severity: keep last half
            if len(self.failures) > 0:
                self.failures = self.failures[len(self.failures)//2:]

    async def is_open(self) -> bool:
        async with self._lock:
            if self.open_until is None:
                return False
            if datetime.utcnow() >= self.open_until:
                # cooldown expired
                self.open_until = None
                self.failures = []
                return False
            return True


# -------------------------
# SMTP connection wrapper that simplifies send_message usage
# -------------------------
class SMTPClientWrapper:
    """
    Wraps aiosmtplib.SMTP and keeps metadata (last_used, healthy)
    """

    def __init__(self, hostname: str, port: int, use_tls: bool, start_tls: bool, tls_context: ssl.SSLContext):
        self.hostname = hostname
        self.port = port
        self.use_tls = use_tls
        self.start_tls = start_tls
        self.tls_context = tls_context
        self.client: Optional[aiosmtplib.SMTP] = None
        self.last_used: Optional[datetime] = None
        self.lock = asyncio.Lock()
        self.healthy = False

    async def create_and_connect(self, username: str, password: str, connect_timeout: int = 5, login_timeout: int = 5) -> bool:
        try:
            client = aiosmtplib.SMTP(
                hostname=self.hostname,
                port=self.port,
                use_tls=self.use_tls,
                start_tls=self.start_tls,
                tls_context=self.tls_context,
            )
            await asyncio.wait_for(client.connect(), timeout=connect_timeout)
            await asyncio.wait_for(client.login(username, password), timeout=login_timeout)
            self.client = client
            self.last_used = datetime.utcnow()
            self.healthy = True
            logger.info(f"SMTP client created: {self.hostname}:{self.port}")
            return True
        except Exception as e:
            logger.error(f"Failed to create/connect SMTP client {self.hostname}:{self.port} - {e}")
            self.healthy = False
            # ensure underlying client closed
            try:
                if self.client:
                    await self.client.close()
            except Exception:
                pass
            self.client = None
            return False

    async def send_message(self, message: EmailMessage, send_timeout: int = 10) -> None:
        if not self.client:
            raise RuntimeError("SMTP client not connected")
        # Using client's send_message directly with wait_for
        await asyncio.wait_for(self.client.send_message(message), timeout=send_timeout)
        self.last_used = datetime.utcnow()

    async def quit(self):
        try:
            if self.client:
                await self.client.quit()
        except Exception:
            try:
                if self.client:
                    await self.client.close()
            except Exception:
                pass
        finally:
            self.client = None
            self.healthy = False


# -------------------------
# Connection Pool Manager
# -------------------------
class SMTPConnectionPool:
    """
    Manages a pool of SMTPClientWrapper instances.
    Pool size controlled via config.
    If pool creation fails for all connections, start() returns False and Mailer will report connection failure.
    """

    def __init__(self, pool_size: int, ports: List[int], hostname: str, username: str, password: str,
                 connect_retries: int = 2, connect_timeout: int = 5, login_timeout: int = 5):
        self.pool_size = max(1, pool_size)
        self._queue: asyncio.Queue[SMTPClientWrapper] = asyncio.Queue()
        self.ports = ports
        self.hostname = hostname
        self.username = username
        self.password = password
        self.connect_retries = connect_retries
        self.connect_timeout = connect_timeout
        self.login_timeout = login_timeout
        self.tls_context = ssl.create_default_context()

    async def start(self) -> bool:
        created = 0
        last_err = None
        # Try to create pool_size clients; try ports fallback if needed
        for i in range(self.pool_size):
            created_client = False
            for port in self.ports:
                use_tls = True if port == 465 else False
                start_tls = not use_tls
                wrapper = SMTPClientWrapper(
                    hostname=self.hostname, port=port, use_tls=use_tls, start_tls=start_tls, tls_context=self.tls_context
                )
                success = False
                for attempt in range(1, self.connect_retries + 2):
                    ok = await wrapper.create_and_connect(self.username, self.password, self.connect_timeout, self.login_timeout)
                    if ok:
                        success = True
                        break
                    else:
                        # backoff
                        await asyncio.sleep(2 ** (attempt - 1))
                if success:
                    await self._queue.put(wrapper)
                    created += 1
                    created_client = True
                    break
                else:
                    last_err = f"Failed to create client on port {port}"
            if not created_client:
                logger.error("Unable to create required pool client (will attempt remaining slots)")
                # continue, attempt next slot
        if created == 0:
            logger.error("No SMTP clients could be created for pool")
            return False
        # If created < pool_size it's OK; pool has partial capacity
        logger.info(f"SMTP Connection Pool started with {created}/{self.pool_size} clients")
        return True

    async def get(self, timeout: Optional[float] = None) -> SMTPClientWrapper:
        # Wait until an available client appears
        if timeout:
            wrapper = await asyncio.wait_for(self._queue.get(), timeout=timeout)
        else:
            wrapper = await self._queue.get()
        return wrapper


    async def put(self, wrapper: SMTPClientWrapper) -> None:
        # Eğer wrapper sağlıksızsa yeniden oluşturmayı dene
        if not wrapper.healthy:
            try:
                await wrapper.quit()
            except:
                pass

            # YENİ EKLENEN KISIM: self-healing SMTP client yeniden yaratma
            recreated = await wrapper.create_and_connect(
                self.username,
                self.password,
                self.connect_timeout,
                self.login_timeout
            )

            if not recreated:
                # bu client tamamen çöktüyse havuza geri koymuyoruz
                logger.warning("SMTP client dropped and could not be recreated")
                return

            logger.info("SMTP client successfully recreated and added back to pool")

        # buraya gelindiyse sağlıklı veya onarılan client havuza eklenir
        await self._queue.put(wrapper)


    async def close_all(self):
        # empty queue and quit all clients
        while not self._queue.empty():
            wrapper = await self._queue.get()
            try:
                await wrapper.quit()
            except Exception:
                pass





# -------------------------
# Main MailerV2
# -------------------------
class MailerV2:
    """High-level mailer with hybrid strategy (connection pool + concurrency control)"""

    DEFAULT_MAX_PARALLEL = DEFAULTS["MAX_PARALLEL_GROUP"]

    def __init__(self, max_parallel: int = None):
        self.max_parallel = max_parallel or _get_conf("MAX_PARALLEL_GROUP")
        self._group_semaphore = asyncio.Semaphore(self.max_parallel)
        # per-domain semaphores
        self._domain_semaphores: Dict[str, asyncio.Semaphore] = {}
        self._pool: Optional[SMTPConnectionPool] = None
        self._started = False
        self._circuit = SimpleCircuitBreaker(
            failure_threshold=_get_conf("CIRCUIT_BREAKER_FAILURES"),
            window_sec=_get_conf("CIRCUIT_BREAKER_WINDOW_SEC"),
            cooldown_sec=_get_conf("CIRCUIT_BREAKER_COOLDOWN_SEC"),
        )
        # read config
        self.ports = _get_conf("SMTP_PORTS")
        self.pool_size = _get_conf("CONNECTION_POOL_SIZE")
        self.per_domain_concurrency = _get_conf("PER_DOMAIN_CONCURRENCY")
        self.connect_timeout = _get_conf("CONNECT_TIMEOUT")
        self.login_timeout = _get_conf("LOGIN_TIMEOUT")
        self.send_timeout = _get_conf("SEND_TIMEOUT")
        self.max_send_retries = _get_conf("MAX_SEND_RETRIES")
        self.connect_retries = _get_conf("CONNECT_RETRIES")
        self.max_attachment_mb = _get_conf("MAX_ATTACHMENT_SIZE_MB")

    # -------------------------
    # Lifecycle
    # -------------------------
    async def start(self) -> bool:
        """Initialize connection pool and mark started. Returns True if pool has at least one client."""
        if self._started:
            return True

        smtp_server = getattr(config.email, 'SMTP_SERVER', None)
        username = getattr(config.email, 'SMTP_USERNAME', None)
        password = getattr(config.email, 'SMTP_PASSWORD', None)

        if not smtp_server or not username or not password:
            logger.error("SMTP configuration missing (SERVER/USERNAME/PASSWORD)")
            return False

        self._pool = SMTPConnectionPool(
            pool_size=self.pool_size,
            ports=self.ports,
            hostname=smtp_server,
            username=username,
            password=password,
            connect_retries=self.connect_retries,
            connect_timeout=self.connect_timeout,
            login_timeout=self.login_timeout,
        )

        ok = await self._pool.start()
        self._started = ok
        return ok

    async def stop(self) -> None:
        """Shutdown pool and mark stopped."""
        if self._pool:
            await self._pool.close_all()
        self._started = False

    # -------------------------
    # Health
    # -------------------------
    async def health(self) -> Dict[str, Any]:
        """Return simple health info."""
        pool_status = {
            "started": self._started,
            "pool_size": self.pool_size,
        }
        circuit_open = await self._circuit.is_open()
        return {"pool": pool_status, "circuit_open": circuit_open}

    # -------------------------
    # Internal send via pool with retries & per-domain concurrency
    # -------------------------
    async def _get_domain(self, recipients: List[str]) -> str:
        # simple domain key: first recipient domain lowercased; used for per-domain concurrency
        if not recipients:
            return "unknown"
        first = recipients[0]
        if "@" in first:
            return first.split("@", 1)[1].lower()
        return "unknown"

    def _get_domain_semaphore(self, domain: str) -> asyncio.Semaphore:
        if domain not in self._domain_semaphores:
            self._domain_semaphores[domain] = asyncio.Semaphore(self.per_domain_concurrency)
        return self._domain_semaphores[domain]

    async def _send_via_pool(self, message: EmailMessage, recipients: List[str]) -> bool:
        """
        Acquire group semaphore & per-domain semaphore, get a connection from pool, send with timeout & retry.
        Returns True on success, False on final failure.
        """
        # circuit breaker check
        if await self._circuit.is_open():
            logger.error("Circuit breaker is open - refusing to send")
            return False

        domain = await self._get_domain(recipients)
        domain_sem = self._get_domain_semaphore(domain)

        async with self._group_semaphore, domain_sem:
            last_err = None
            for attempt in range(1, self.max_send_retries + 2):
                if not self._pool:
                    raise RuntimeError("Connection pool not initialized")

                try:
                    # get connection with small timeout to avoid blocking forever
                    wrapper = await self._pool.get(timeout=10)
                except asyncio.TimeoutError:
                    last_err = "No SMTP client available from pool"
                    logger.error(last_err)
                    await self._circuit.record_failure()
                    return False

                try:
                    # Ensure message From header
                    if 'From' not in message:
                        message['From'] = getattr(config.email, 'SMTP_USERNAME')

                    # send using wrapper with per-send timeout
                    await asyncio.wait_for(wrapper.send_message(message, send_timeout=self.send_timeout), timeout=self.send_timeout + 2)
                    # put back healthy wrapper
                    await self._pool.put(wrapper)
                    await self._circuit.record_success()
                    logger.info(f"Mail sent to {recipients}")
                    return True
                except Exception as e:
                    last_err = e
                    logger.error(f"Send attempt {attempt} failed for {recipients}: {e}")
                    # mark wrapper unhealthy and do not put it back as healthy
                    wrapper.healthy = False
                    try:
                        await wrapper.quit()
                    except Exception:
                        pass
                    # do not put back - pool.put will attempt recreation if healthy flag is False
                    # backoff before next attempt
                    await asyncio.sleep(2 ** (attempt - 1))
                    await self._circuit.record_failure()
                    # continue retry
                finally:
                    # If wrapper still exists and not in queue, try to ensure pool size
                    # Note: pool.put handles recreation
                    pass

            logger.error(f"All send attempts failed for {recipients}: {last_err}")
            return False

    # -------------------------
    # Public/simple API (preserve methods names)
    # -------------------------
    async def send_simple_email(self, to_emails: List[str], subject: str, body: str, max_retries: int = None) -> bool:
        if not to_emails or not any(to_emails):
            logger.warning("No recipients for simple email")
            return False

        msg = EmailMessage()
        msg['From'] = getattr(config.email, 'SMTP_USERNAME')
        msg['To'] = ', '.join(to_emails)
        msg['Subject'] = subject
        msg.set_content(body)

        return await self._send_via_pool(msg, to_emails)

    def _attach_file_to_message(self, msg: EmailMessage, path: Path) -> bool:
        """
        Attach a file to message after checking size limits.
        Returns True if attached, False if skipped.
        """
        if not path or not path.exists():
            logger.error(f"Attachment not found: {path}")
            return False

        size = path.stat().st_size
        if _bytes_to_mb(size) > self.max_attachment_mb:
            logger.warning(f"Attachment {path.name} too large ({_bytes_to_mb(size):.2f}MB) > {self.max_attachment_mb}MB")
            return False

        ctype, encoding = mimetypes.guess_type(str(path))
        if ctype is None:
            ctype = 'application/octet-stream'
        maintype, subtype = ctype.split('/', 1)
        # read file into memory (reasonable because we enforce limit)
        with path.open('rb') as f:
            data = f.read()
        msg.add_attachment(data, maintype=maintype, subtype=subtype, filename=path.name)
        logger.debug(f"Attached {path.name} ({ctype}) size={_bytes_to_mb(size):.2f}MB")
        return True

    async def send_email_with_attachment(self, to_emails: List[str], subject: str, body: str, attachment_path: Path, max_retries: int = None) -> bool:
        if not to_emails or not any(to_emails):
            logger.warning("No recipients")
            return False
        if not attachment_path or not attachment_path.exists():
            logger.error(f"Attachment not found: {attachment_path}")
            return False

        msg = EmailMessage()
        msg['From'] = getattr(config.email, 'SMTP_USERNAME')
        msg['To'] = ', '.join(to_emails)
        msg['Subject'] = subject
        msg.set_content(body)

        attached = self._attach_file_to_message(msg, attachment_path)
        if not attached:
            # Attachment too big or missing; don't send attachment - caller can handle link strategy
            logger.error(f"Attachment not attached (size/missing): {attachment_path}")
            return False

        return await self._send_via_pool(msg, to_emails)

    async def send_email_with_multiple_attachments(self, to_emails: List[str], subject: str, body: str, attachment_paths: List[Path], max_retries: int = None) -> bool:
        if not to_emails or not any(to_emails):
            logger.warning("No recipients")
            return False
        if not attachment_paths:
            logger.warning("No attachment paths provided")
            return False

        msg = EmailMessage()
        msg['From'] = getattr(config.email, 'SMTP_USERNAME')
        msg['To'] = ', '.join(to_emails)
        msg['Subject'] = subject
        msg.set_content(body)

        attached = 0
        for p in attachment_paths:
            if p and p.exists():
                ok = self._attach_file_to_message(msg, p)
                if ok:
                    attached += 1
            else:
                logger.warning(f"Attachment missing: {p}")

        if attached == 0:
            logger.error("No valid attachments to send")
            return False

        return await self._send_via_pool(msg, to_emails)

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

    async def send_automatic_bulk_email(self, input_path: Path, output_files: Dict[str, Dict[str, Any]], processing_report: str = "", max_retries: int = None) -> bool:
        personal = getattr(config.email, 'PERSONAL_EMAIL', None)
        if not personal:
            logger.error("PERSONAL_EMAIL not configured")
            return False

        zip_path = await self._create_bulk_zip(input_path, output_files)
        if not zip_path:
            return False

        subject = f"📊 TelData Raporu - {input_path.name}"
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

    async def send_input_only_email(self, input_path: Path, max_retries: int = None) -> bool:
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

    # -------------------------
    # Batch orchestration (keeps method name send_batch)
    # -------------------------
    async def send_batch(self, jobs: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Jobs: list of dicts with at least:
          - type: "group" | "input" | "bulk"
          - to, subject, body, attachments (list) or attachments[0] for legacy
          - meta for bulk: input_path, output_files
        Returns results list aligned by job index.
        """
        results: List[Optional[Dict[str, Any]]] = [None] * len(jobs)

        # Ensure started/pool available
        if not self._started:
            connected = await self.start()
            if not connected:
                for i in range(len(jobs)):
                    results[i] = {"job_index": i, "success": False, "error": "Mailer cannot connect"}
                return results

        # 1) Launch group jobs (parallel limited by semaphores)
        group_tasks = {}
        for i, job in enumerate(jobs):
            if job.get("type") == "group":
                to = job.get("to")
                subj = job.get("subject", "")
                body = job.get("body", "")
                att = job.get("attachments", [])
                # legacy: attachments might be a path string
                attachment_path = None
                if att:
                    a = att[0]
                    if isinstance(a, (str, Path)):
                        attachment_path = Path(a)
                        
                # create EmailMessage once, attach later in coroutine
                async def group_send_coro(index: int, to_emails: List[str], subject: str, body_text: str, attachment_p: Optional[Path]):
                    try:
                        if attachment_p:
                            ok = await self.send_email_with_attachment(to_emails, subject, body_text, attachment_p)
                        else:
                            ok = await self.send_simple_email(to_emails, subject, body_text)
                        return {"job_index": index, "success": bool(ok)}
                    except Exception as e:
                        return {"job_index": index, "success": False, "error": str(e)}

                coro = group_send_coro(i, to, subj, body, attachment_path)
                task = asyncio.create_task(coro)
                group_tasks[task] = i

        # Wait for group tasks to finish (no global 6-minute wait). We'll await gather with return_exceptions.
        if group_tasks:
            done = await asyncio.gather(*group_tasks.keys(), return_exceptions=True)
            for task, outcome in zip(group_tasks.keys(), done):
                job_index = group_tasks[task]
                if isinstance(outcome, Exception):
                    results[job_index] = {"job_index": job_index, "success": False, "error": str(outcome)}
                else:
                    results[job_index] = outcome

        # 2) INPUT mails (serial)
        for i, job in enumerate(jobs):
            if job.get("type") == "input":
                try:
                    to = job.get("to")
                    subj = job.get("subject", "")
                    body = job.get("body", "")
                    att = job.get("attachments", [])
                    if att:
                        attachment_path = Path(att[0]) if isinstance(att[0], (str, Path)) else None
                        ok = await self.send_email_with_attachment(to, subj, body, attachment_path) if attachment_path else await self.send_simple_email(to, subj, body)
                    else:
                        ok = await self.send_simple_email(to, subj, body)
                    results[i] = {"job_index": i, "success": bool(ok)}
                except Exception as e:
                    results[i] = {"job_index": i, "success": False, "error": str(e)}

        # 3) BULK mails (serial)
        for i, job in enumerate(jobs):
            if job.get("type") == "bulk":
                try:
                    input_path = job.get("meta", {}).get("input_path")
                    output_files = job.get("meta", {}).get("output_files")
                    zip_path = await self._create_bulk_zip(Path(input_path) if input_path else None, output_files)
                    if not zip_path:
                        results[i] = {"job_index": i, "success": False, "error": "ZIP creation failed"}
                        continue
                    to = job.get("to")
                    subj = job.get("subject", "")
                    body = job.get("body", "")
                    ok = await self.send_email_with_attachment(to, subj, body, zip_path)
                    results[i] = {"job_index": i, "success": bool(ok)}
                    try:
                        if zip_path.exists():
                            zip_path.unlink()
                    except Exception:
                        pass
                except Exception as e:
                    results[i] = {"job_index": i, "success": False, "error": str(e)}

        return results


# -------------------------
# End of Mailer V6
# -------------------------
