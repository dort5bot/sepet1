# handlers/dar_handler.py# handlers/dar_handler.py
"""
v8
# handlers/dar_handler.py
komut aciklaması yok (commat_info)> aktif dönemde anlamlı 
Aiogram 3.x uyumlu
Proje yedekleme ve komut tarama yardımcı handler
. ile başlayan dosyalar ve __pycache__ gibi klasörler yok sayılır.
/dar    → Proje ağacı
/dar k  → Komut listesi
/dar k → tüm @router.message(Command(...)) komutlarını bulur
/dar t → proje ağaç yapısı+dosyaların içeriğini birleştirir,
            her dosya için başlık ekler .txt dosyası olarak gönderir.
/dar t <dosya> → Sadece belirtilen dosya
    /dar t main.py →  Sadece main.py
/dar t <klasör> → Sadece belirtilen klasördeki .py dosyaları
    /dar t handlers → handlers klasöründeki tüm dosyalar
/dar z   → Filtrelenmiş ZIP yedek +dosya agacı (tree.txt)
/dar id  → id bilgisi
# zaman format: mbot1_0917_2043 (aygün_saaddkika) ESKİ: "%Y%m%d_%H%M%S" = YılAyGün_SaatDakikaSaniye
+
Çoklu Dosya Formatı Desteği (.py, .txt, .md, .json, .yaml, vs.)
Önemli Dosyalar (Dockerfile, requirements.txt, .env, vs.)
"""

# handlers/dar_handler.py
import logging
import os
import re
import zipfile
import tempfile
from pathlib import Path
from datetime import datetime
from typing import Optional, List

from aiogram import Router
from aiogram.types import Message, FSInputFile
from aiogram.filters import Command, CommandObject

logger = logging.getLogger(__name__)
router = Router(name="dar_handler")

# ---------------- CONFIG ----------------
PROJECT_ROOT = Path(os.getenv("BOT_ROOT", Path(__file__).resolve().parents[1])).resolve()
TMP_DIR = Path(tempfile.gettempdir())
TMP_DIR.mkdir(parents=True, exist_ok=True)

TELEGRAM_NAME = os.getenv("TELEGRAM_NAME", "hbot")
TELEGRAM_MSG_LIMIT = 4000

ALLOWED_EXTENSIONS = {
    ".py", ".yaml", ".json",
}
ALLOWED_FILENAMES = {
    "Dockerfile", "requirements.txt", ".gitignore", "docker-deploy.yml"
}
EXCLUDED_DIRS = {"__pycache__", ".git", ".venv", "venv", "node_modules"}

# ---------------- FILTER ----------------
def should_include_file(path: Path) -> bool:
    if path.name.startswith("."):
        return False
    if any(p in EXCLUDED_DIRS for p in path.parts):
        return False
    if path.name in ALLOWED_FILENAMES:
        return True
    return path.suffix in ALLOWED_EXTENSIONS

# ---------------- TREE (ASLA BOZULMAZ) ----------------
def generate_tree_numbered(path: Path, prefix: str = "") -> List[str]:
    lines = []
    dirs, files = [], []

    for entry in sorted(path.iterdir(), key=lambda e: e.name.lower()):
        if entry.name.startswith(".") or entry.name in EXCLUDED_DIRS:
            continue
        (dirs if entry.is_dir() else files).append(entry)

    for idx, entry in enumerate(dirs + files, start=1):
        num = f"{prefix}{idx}"
        if entry.is_dir():
            lines.append(f"{num} {entry.name}/")
            lines.extend(generate_tree_numbered(entry, num + "."))
        else:
            lines.append(f"{num} {entry.name}")
    return lines

# ---------------- FIND TARGET ----------------
def find_file_or_folder(root: Path, name: str) -> Optional[Path]:
    name = name.lower()
    for p in root.rglob("*"):
        if p.name.lower() == name and not p.name.startswith("."):
            return p
    return None

def collect_files(target: Path) -> List[Path]:
    files = []
    if target.is_file() and should_include_file(target):
        return [target]
    if target.is_dir():
        for p in target.rglob("*"):
            if p.is_file() and should_include_file(p):
                files.append(p)
    return sorted(files)

# ---------------- COMMAND SCAN ----------------
def scan_handlers_for_commands():
    result = {}
    handler_dir = PROJECT_ROOT / "handlers"
    pattern = re.compile(r'Command\(["\'](\w+)["\']')

    for f in handler_dir.glob("*.py"):
        try:
            content = f.read_text(encoding="utf-8")
            for cmd in pattern.findall(content):
                result[f"/{cmd}"] = f"({f.name})"
        except Exception:
            pass
    return result

# ---------------- HANDLER ----------------
@router.message(Command("dar"))
async def dar_command(message: Message, command: CommandObject = None):
    args = command.args.split() if command and command.args else []
    mode = args[0].lower() if args else ""
    timestamp = datetime.now().strftime("%m%d_%H%M")


    # --- ID Sorgulama (/dar i)
    # ---------------------------
    if mode == "i":
        if not message.from_user:
            await message.answer("Kullanıcı bilgisi alınamadı.")
            return

        user_id = message.from_user.id
        response = f"👤 Telegram ID:\n<code>{user_id}</code>"
        await message.answer(response, parse_mode="HTML")
        return



    # ---------- /dar k ----------
    # ---------------------------
    if mode == "k":
        cmds = scan_handlers_for_commands()
        text = "\n".join(f"{k} → {v}" for k, v in sorted(cmds.items()))
        await message.answer(f"<pre>{text}</pre>", parse_mode="HTML")
        return

    # ---------- TREE ----------
    tree_lines = generate_tree_numbered(PROJECT_ROOT)
    tree_text = "\n".join(tree_lines)

    # ---------- /dar t ----------
    # ---------------------------
    if mode == "t":
        target_name = args[1] if len(args) > 1 else None
        blocks = ["📁 PROJE AĞAÇ YAPISI\n", tree_text, "\n" + "=" * 50 + "\n"]

        if target_name:
            target = find_file_or_folder(PROJECT_ROOT, target_name)
            if not target:
                await message.answer(f"❌ '{target_name}' bulunamadı")
                return
            files = collect_files(target)
        else:
            files = collect_files(PROJECT_ROOT)

        for f in files:
            rel = f.relative_to(PROJECT_ROOT)
            try:
                content = f.read_text(encoding="utf-8")
            except Exception:
                continue
            blocks.append(
                f"\n{'='*30}\n|| {rel.as_posix()} ||\n{'='*30}\n{content.strip()}\n"
            )

        full_text = "\n".join(blocks)

        if len(full_text) > TELEGRAM_MSG_LIMIT:
            txt = TMP_DIR / f"{TELEGRAM_NAME}_{timestamp}.txt"
            txt.write_text(full_text, encoding="utf-8")
            await message.answer_document(FSInputFile(str(txt)))
            txt.unlink(missing_ok=True)
        else:
            await message.answer(f"<pre>{full_text}</pre>", parse_mode="HTML")
        return

    # ---------- /dar z ----------
    # ---------------------------
    if mode == "z":
        zip_path = TMP_DIR / f"{TELEGRAM_NAME}_{timestamp}.zip"
        try:
            with zipfile.ZipFile(zip_path, "w", zipfile.ZIP_DEFLATED) as z:
                # tree.txt ekle
                z.writestr("tree.txt", tree_text)

                for root, _, files in os.walk(PROJECT_ROOT):
                    for name in files:
                        p = Path(root) / name
                        if should_include_file(p):
                            z.write(p, p.relative_to(PROJECT_ROOT))

            await message.answer_document(FSInputFile(str(zip_path)))
        finally:
            zip_path.unlink(missing_ok=True)
        return


    # ---------- /dar ----------
    # ---------------------------
    if len(tree_text) > TELEGRAM_MSG_LIMIT:
        txt = TMP_DIR / f"{TELEGRAM_NAME}_{timestamp}.txt"
        txt.write_text(tree_text, encoding="utf-8")
        await message.answer_document(FSInputFile(str(txt)))
        txt.unlink(missing_ok=True)
    else:
        await message.answer(f"<pre>{tree_text}</pre>", parse_mode="HTML")
