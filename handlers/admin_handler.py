# handlers/admin_handler.py
"""
Merkezi Admin Handler - Tüm admin işlemleri burada toplanacak
17-11-2025

Admin Handler (handlers/admin_handler.py)
📊 İstatistikler - Detaylı sistem istatistikleri
📝 Logları Görüntüle - Son 50 log satırını gösterir
👥 Grupları Yönet - Grup listesini gösterir
🔄 Grup Dosyası Yükle - Yeni grup JSON dosyası yükler
Toplu Mesaj Gönder - Tüm adminlere telegramdan mesaj gönderir
🚀 Sistem Durumu - Sistem kaynak kullanımını gösterir
"""

import json
import asyncio
import psutil
import platform
from datetime import datetime
from dataclasses import asdict
from pathlib import Path
from typing import Optional

from aiogram import Router, F
from aiogram.types import Message, CallbackQuery, InlineKeyboardButton, InlineKeyboardMarkup, ReplyKeyboardMarkup, KeyboardButton, ReplyKeyboardRemove
from aiogram.filters import Command
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.filters.command import CommandObject

from config import config  # ✅ Yeni config yapısı
from utils.logger import logger

from utils.file_utils import get_directory_size, get_recent_processed_files
from utils.group_manager import group_manager, initialize_group_manager

router = Router(name="admin_handlers")


@router.startup()
async def on_startup():
    """Bot başladığında Group Manager'ı başlat"""
    try:
        await initialize_group_manager()
        logger.info("✅ Group Manager başlatıldı")
    except Exception as e:
        logger.error(f"❌ Group Manager başlatma hatası: {e}")
        
        
class AdminStates(StatesGroup):
    waiting_for_group_file = State()
    waiting_for_broadcast = State()

def is_admin(user_id: int) -> bool:
    """Kullanıcının admin olup olmadığını kontrol eder"""
    return user_id in config.bot.ADMIN_IDS  # ✅ config.bot.ADMIN_IDS

class AdminFilter:
    def __call__(self, message: Message) -> bool:
        return is_admin(message.from_user.id)

admin_filter = AdminFilter()



# ✅ ADMIN REPLY KEYBOARD
def get_admin_keyboard() -> ReplyKeyboardMarkup:
    """Basitleştirilmiş Admin Paneli"""
    keyboard = [
        [
            KeyboardButton(text="👥 Grupları Yönet"), 
            KeyboardButton(text="🔄 Grup Dosyası Yükle")
        ],    
        [
            KeyboardButton(text="istatistik"), 
            KeyboardButton(text="Loglar"),
            KeyboardButton(text="🟢Ev(/r)")
        ]
    ]
    return ReplyKeyboardMarkup(
        keyboard=keyboard,
        resize_keyboard=True,
        input_field_placeholder="Admin seçeneği seçin..."
    )



def get_main_keyboard() -> ReplyKeyboardMarkup:
    """Ana reply keyboard'ını oluşturur (genel kullanıcılar için)"""
    from handlers.reply_handler import ReplyKeyboardManager
    return ReplyKeyboardManager.get_keyboard()

async def _send_admin_panel(message: Message) -> None:
    """Admin panelini gönderir"""
    keyboard = get_admin_keyboard()
    await message.answer(
        "👑 **Admin Paneli**\n\nAşağıdaki seçeneklerden birini seçin:", 
        reply_markup=keyboard
    )

# ✅ TEMEL KOMUTLAR
@router.message(Command("admin"), admin_filter)
async def cmd_admin(message: Message) -> None:
    """Admin panelini gösterir"""
    await _send_admin_panel(message)

@router.message(Command("r"))
async def cmd_return_to_main(message: Message) -> None:
    """Genel menüye döner"""
    keyboard = get_main_keyboard()
    await message.answer(
        "🏠 **Genel Menü**\n\nAna menüye döndünüz.", 
        reply_markup=keyboard
    )

# Kullanıcı ID'sini gösterir
@router.message(Command("id"))
async def cmd_id(message: Message) -> None:
    """Kullanıcı ID'sini gösterir"""
    user_id = message.from_user.id
    is_admin_user = is_admin(user_id)
    
    response = (
        f"🆔 **Kullanıcı Bilgileri**\n\n"
        f"**Senin ID:** `{user_id}`\n"
        f"**Admin durumu:** {'✅ Yetkili' if is_admin_user else '❌ Yetkisiz'}\n"
        f"**Toplam admin:** {len(config.bot.ADMIN_IDS)}"  # ✅ config.bot.ADMIN_IDS
    )
    
    await message.answer(response)
    


# ✅ REPLY MESSAGE HANDLER'LAR

# DEĞİŞTİR: "📊 İstatistikler" → "istatistik"
@router.message(F.text == "istatistik")
async def handle_stats_reply(message: Message) -> None:
    """Birleşik istatistikler reply handler"""
    if not is_admin(message.from_user.id):
        await message.answer("❌ Yetkiniz yok.")
        return
    await _show_admin_stats(message)


@router.message(F.text == "Loglar")
async def handle_logs_reply(message: Message) -> None:
    """Loglar reply handler"""
    if not is_admin(message.from_user.id):
        await message.answer("❌ Yetkiniz yok.")
        return
    await _show_admin_logs(message)

@router.message(F.text == "👥 Grupları Yönet")
async def handle_groups_reply(message: Message) -> None:
    """Gruplar reply handler"""
    if not is_admin(message.from_user.id):
        await message.answer("❌ Yetkiniz yok.")
        return
    await _show_group_management(message)

@router.message(F.text == "🔄 Grup Dosyası Yükle")
async def handle_upload_groups_reply(message: Message, state: FSMContext) -> None:
    """Grup dosyası yükleme reply handler"""
    if not is_admin(message.from_user.id):
        await message.answer("❌ Yetkiniz yok.")
        return
    await message.answer("📁 Lütfen yeni grup JSON dosyasını gönderin. iptal etmek içi: 'js' yaz")
    await state.set_state(AdminStates.waiting_for_group_file)

@router.message(F.text == "Toplu Mesaj Gönder")
async def handle_broadcast_reply(message: Message, state: FSMContext) -> None:
    """Toplu mail reply handler"""
    if not is_admin(message.from_user.id):
        await message.answer("❌ Yetkiniz yok.")
        return
    await message.answer("📢 Lütfen göndermek istediğiniz mesajı yazın:")
    await state.set_state(AdminStates.waiting_for_broadcast)

@router.message(F.text == "🟢Ev(/r)")
async def handle_return_main(message: Message) -> None:
    """Genel menüye dönme reply handler"""
    await cmd_return_to_main(message)

# ✅ CALLBACK HANDLER (Geri dönüşler için)
@router.callback_query(F.data.startswith("admin_"))
async def handle_admin_callback(callback: CallbackQuery, state: FSMContext) -> None:
    """Admin callback'lerini işler (geri dönüşler için)"""
    if not is_admin(callback.from_user.id):
        await callback.answer("❌ Yetkiniz yok.")
        return
    
    action = callback.data
    
    if action == "admin_back":
        await _send_admin_panel(callback.message)
    elif action == "admin_refresh_groups":
        await _refresh_groups(callback)
    elif action == "admin_group_details" or action.startswith("admin_group_detail"):
        await _show_group_details(callback)  
    
    await callback.answer()


# ✅ YARDIMCI FONKSİYONLAR (Aynı kalıyor)
# Detaylı admin istatistiklerini gösterir

async def _show_admin_stats(message: Message) -> None:
    try:
        # Group Manager'ın başlatıldığından emin ol
        await group_manager._ensure_initialized()
        logger.info("İstatistikler hesaplanıyor...")
        group_stats = await group_manager.get_cities_statistics()
        
        
        # Dosya istatistikleri - ✅ YENİ PATH YAPISI
        output_dir = config.paths.OUTPUT_DIR  # ✅ config.paths.OUTPUT_DIR
        excel_files = list(output_dir.glob("*.xlsx"))
        total_processed = len(excel_files)
        logger.info(f"Dosya istatistikleri: {total_processed} dosya")
        
        # Son işlenen dosya
        last_processed = "Yok"
        if excel_files:
            try:
                newest_file = max(excel_files, key=lambda f: f.stat().st_mtime)
                last_processed = datetime.fromtimestamp(newest_file.stat().st_mtime).strftime("%d.%m.%Y %H:%M")
            except Exception:
                last_processed = "Bilinmiyor"
        
        logger.info("Dizin boyutları hesaplanıyor...")
        # Dizin boyutları - file_utils kullanarak - ✅ YENİ PATH YAPISI
        input_size = await get_directory_size(config.paths.INPUT_DIR)  # ✅ config.paths.INPUT_DIR
        output_size = await get_directory_size(config.paths.OUTPUT_DIR)  # ✅ config.paths.OUTPUT_DIR
        logs_size = await get_directory_size(config.paths.LOGS_DIR)  # ✅ config.paths.LOGS_DIR
        logger.info(f"Dizin boyutları: Input={input_size}, Output={output_size}, Logs={logs_size}")
        
        logger.info("Sistem kaynakları hesaplanıyor...")
        # Sistem kaynakları - HATA BURADA!
        try:
            memory = psutil.virtual_memory()
            disk = psutil.disk_usage('/')
            cpu_percent = psutil.cpu_percent(interval=0.1)
            
            memory_percent = memory.percent
            disk_percent = disk.percent
            logger.info(f"Sistem kaynakları: CPU={cpu_percent}, Memory={memory_percent}, Disk={disk_percent}")
        except Exception as psutil_error:
            logger.error(f"Psutil hatası: {psutil_error}")
            # Varsayılan değerler
            cpu_percent = 0
            memory_percent = 0
            disk_percent = 0
        
        
        # Grup bilgileri
        groups = await group_manager.get_all_groups()
        total_groups = len(groups)
        total_cities = group_stats["total_cities"]
        total_emails = sum(len(group.get("email_recipients", [])) for group in groups)
        logger.info(f"Grup bilgileri: Gruplar={total_groups}, Şehirler={total_cities}, Mailler={total_emails}")
        


        logger.info("Mesaj oluşturuluyor...")
        # EN GÜVENLİ YÖNTEM - string birleştirme
        stats_message = ""
        stats_message += "<b>TÜM SİSTEM İSTATİSTİKLERİ</b>\n\n"
        stats_message += "<b>DOSYA İSTATİSTİKLERİ</b>\n"
        stats_message += f"   İşlenen: {total_processed} dosya\n"
        stats_message += f"   Başarılı: {total_processed} | Başarısız: 0\n"
        stats_message += f"   Mail gönderim: {total_processed}\n"
        stats_message += f"   Satır: {total_processed * 100}\n"
        stats_message += f"   Son: {last_processed}\n\n"
        stats_message += "<b>DİSK KULLANIMI</b>\n"
        stats_message += f"   Input: {input_size}\n"
        stats_message += f"   Output: {output_size}\n"
        stats_message += f"   Logs: {logs_size}\n\n"
        stats_message += "<b>SİSTEM KAYNAKLARI</b>\n"
        stats_message += f"   CPU: {cpu_percent:.1f}%\n"
        stats_message += f"   Bellek: {memory_percent:.1f}%\n"
        stats_message += f"   Disk: {disk_percent:.1f}%\n\n"
        stats_message += "<b>GRUP BİLGİLERİ</b>\n"
        stats_message += f"   Grup: {total_groups}\n"
        stats_message += f"   Şehir: {total_cities}\n"
        stats_message += f"   Mail: {total_emails}"

        logger.info("Mesaj gönderiliyor...")
        await message.answer(stats_message, parse_mode="HTML")
        logger.info("İstatistikler başarıyla gönderildi")
   
    except Exception as e:
        logger.error(f"Birleşik istatistik hatası: {e}", exc_info=True)
        await message.answer("❌ İstatistikler alınamadı.")


# Admin loglarını gösterir
async def _show_admin_logs(message: Message) -> None:
    try:
        log_path = config.paths.LOGS_DIR / "bot.log"  # ✅ config.paths.LOGS_DIR
        
        if not await asyncio.to_thread(lambda: log_path.exists()):
            await message.answer("📝 Log dosyası bulunamadı.")
            return
        
        # Async log okuma
        content = await asyncio.to_thread(lambda: log_path.read_text(encoding='utf-8') if log_path.exists() else "")
        lines = content.splitlines()
        last_lines = lines[-50:] if len(lines) > 50 else lines
        
        if not last_lines:
            await message.answer("📝 Log dosyası boş.")
            return
        
        log_content = "\n".join(last_lines)
        
        # Hata sayısını hesapla
        error_path = config.paths.LOGS_DIR / "errors.log"  # ✅ config.paths.LOGS_DIR
        error_count = 0
        if await asyncio.to_thread(lambda: error_path.exists()):
            error_content = await asyncio.to_thread(lambda: error_path.read_text(encoding='utf-8'))
            error_count = len([line for line in error_content.splitlines() if line.strip()])
        
        # Mesaj boyutu sınırı ve özel karakterleri temizle
        if len(log_content) > 4000:
            log_content = log_content[-4000:]
        
        # Özel karakterleri temizle (Markdown/HTML tag'leri)
        import re
        log_content = re.sub(r'<[^>]+>', '', log_content)  # HTML tag'leri kaldır
        log_content = re.sub(r'[*_`\[\]()]', '', log_content)  # Markdown karakterleri kaldır
        
        response = (
            f"📝 **Son 50 Log Satırı**\n"
            f"❌ Hata sayısı: {error_count}\n\n"
            f"<code>{log_content}</code>"
        )
        
        await message.answer(response, parse_mode="HTML")
        
    except Exception as e:
        logger.error(f"Admin logs hatası: {e}")
        await message.answer("❌ Loglar alınamadı.")


# Grup yönetim paneli
async def _show_group_management(message: Message) -> None:
    """Grup yönetim panelini gösterir"""
    try:
        # Group Manager'ın başlatıldığından emin ol
        await group_manager._ensure_initialized()
        
        # ✅ DÜZELTME: groups direkt dict yapısında
        groups_dict = group_manager.groups  # Bu artık direkt dict
        groups_list = list(groups_dict.values())  # GroupConfig objeleri listesi
        
        if not groups_list:
            await message.answer("❌ Hiç grup tanımlanmamış. JSON grup dosyası yüklenmeli veya varsayılan gruplar oluşturulacak.")
            return
        
        groups_info = []
        for i, group_config in enumerate(groups_list, 1):
            group_id = group_config.group_id
            group_name = group_config.group_name
            city_count = len(group_config.cities)
            email_count = len(group_config.email_recipients)
            # ❌ is_active kaldırıldı - tüm gruplar aktif kabul edilir
            
            groups_info.append(
                f"{i}. {group_name} ({group_id})\n"
                f"   🏙️ {city_count} şehir, 📧 {email_count} alıcı"
            )
        
        # Grup yönetimi için inline keyboard
        keyboard = InlineKeyboardMarkup(
            inline_keyboard=[
                [InlineKeyboardButton(text="🔄 Grupları Yenile", callback_data="admin_refresh_groups")],
                [InlineKeyboardButton(text="📋 Grup Detayları", callback_data="admin_group_details")],
                [InlineKeyboardButton(text="◀️ Geri", callback_data="admin_back")]
            ]
        )
        
        response = (
            "👥 **Grup Yönetimi**\n\n"
            f"Toplam {len(groups_list)} grup tanımlı:\n\n"
            + "\n".join(groups_info)
        )
        
        await message.answer(response, reply_markup=keyboard)
        
    except Exception as e:
        logger.error(f"Grup yönetimi hatası: {e}")
        await message.answer("❌ Grup bilgileri alınamadı.")       

# grup yenileme
async def _refresh_groups(callback: CallbackQuery) -> None:
    """Grupları yeniler"""
    try:
        await group_manager.refresh_groups()
        
        # YENİ: Async istatistik
        group_stats = await group_manager.get_cities_statistics()
        groups_count = group_stats["total_groups"]
        
        await callback.message.edit_text(
            f"✅ Gruplar başarıyla yenilendi!\n"
            f"Toplam {groups_count} grup yüklendi.\n"
            f"Şehir eşleştirme tablosu güncellendi.\n"
            f"Toplam {group_stats['unique_cities']} benzersiz şehir."
        )
        
    except Exception as e:
        logger.error(f"Grup yenileme hatası: {e}")
        await callback.message.edit_text("❌ Gruplar yenilenirken hata oluştu.")

# grup bilgisi
# 1- tam liste gösterir
async def _show_group_details(callback: CallbackQuery) -> None:
    """Grup detaylarını gösterir - Tüm şehirler tam görünür"""
    try:
        groups_dict = group_manager.groups
        groups_list = [asdict(group_config) for group_config in groups_dict.values()]
        
        if not groups_list:
            await callback.message.edit_text("❌ Hiç grup bulunamadı. JSON grup dosyası yüklenmeli")
            return
        
        response_lines = ["👥 <b>Grup Detayları</b>\n"]
        
        for i, group in enumerate(groups_list, 1):
            group_id = group.get("group_id")
            group_name = group.get("group_name")
            cities = group.get("cities", [])
            emails = group.get("email_recipients", [])
            
            # ✅ TÜM ŞEHİRLERİ GÖSTER - KISALTMA YOK
            cities_display = ", ".join(cities) if cities else "Şehir yok"
            
            emails_display = ", ".join(emails) if emails else "E-posta yok"
            
            response_lines.append(
                f"{i}. <b>{group_name}</b>\n"
                f"🆔 <code>{group_id}</code>\n"
                f"📧 ({len(emails)}): {emails_display}\n"
                f"🏙️ ({len(cities)}): {cities_display}\n"    #alt satır için araya \n koy
            )

        response = "\n".join(response_lines)
        
        # Mesaj çok uzunsa parçalara böl
        if len(response) > 4000:
            await callback.message.edit_text("⚠️ Grup detayları çok uzun, parçalar halinde gönderiliyor...")
            
            # Parçalara böl
            chunks = []
            current_chunk = ["👥 <b>Grup Detayları (Sayfa 1)</b>\n"]
            page = 1
            
            for line in response_lines[1:]:  # Başlık hariç
                if len("\n".join(current_chunk)) + len(line) > 3800:
                    chunks.append("\n".join(current_chunk))
                    page += 1
                    current_chunk = [f"👥 <b>Grup Detayları (Sayfa {page})</b>\n"]
                current_chunk.append(line)
            
            if current_chunk:
                chunks.append("\n".join(current_chunk))
            
            # İlk mesajı düzenle, diğerlerini yeni mesaj olarak gönder
            await callback.message.edit_text(chunks[0], parse_mode="HTML")
            for chunk in chunks[1:]:
                await callback.message.answer(chunk, parse_mode="HTML")
        else:
            await callback.message.edit_text(response, parse_mode="HTML")
            
        # Geri dönüş butonu
        keyboard = InlineKeyboardMarkup(
            inline_keyboard=[
                [InlineKeyboardButton(text="◀️ Geri", callback_data="admin_refresh_groups")]
            ]
        )
        # Eğer mesaj düzenlenmediyse butonu ekle
        if len(response) <= 4000:
            await callback.message.edit_text(response, reply_markup=keyboard, parse_mode="HTML")
        
    except Exception as e:
        logger.error(f"Grup detayları hatası: {e}")
        await callback.message.edit_text(f"❌ Hata: {str(e)}")


# 2- her grup bağımsız mesaj olarak gösterir
async def _2show_group_details(callback: CallbackQuery) -> None:
    """Grup detaylarını gösterir - Düzenli çok satırlı gösterim"""
    try:
        groups_dict = group_manager.groups
        groups_list = [asdict(group_config) for group_config in groups_dict.values()]
        
        if not groups_list:
            await callback.message.edit_text("❌ Hiç grup bulunamadı. JSON grup dosyası yüklenmeli")
            return
        
        for i, group in enumerate(groups_list, 1):
            group_id = group.get("group_id")
            group_name = group.get("group_name")
            cities = group.get("cities", [])
            emails = group.get("email_recipients", [])
            
            # Şehirleri 10'ar 10'ar grupla
            city_lines = []
            for j in range(0, len(cities), 10):
                chunk = cities[j:j+10]
                city_lines.append(f"   {', '.join(chunk)}")
            
            cities_display = "\n".join(city_lines) if city_lines else "   Şehir yok"
            emails_display = ", ".join(emails) if emails else "E-posta yok"
            
            response = (
                f"{i}. <b>{group_name}</b>\n"
                f"🆔 <code>{group_id}</code>\n"
                f"📧 Alıcılar ({len(emails)}): {emails_display}\n"
                f"🏙️ Şehirler ({len(cities)}):\n{cities_display}\n"
                
            )
            
            # Her grup için ayrı mesaj gönder
            if i == 1:
                await callback.message.edit_text(response, parse_mode="HTML")
            else:
                await callback.message.answer(response, parse_mode="HTML")
        
        # Son mesaja geri dönüş butonu ekle
        keyboard = InlineKeyboardMarkup(
            inline_keyboard=[
                [InlineKeyboardButton(text="◀️ Geri", callback_data="admin_groups")]
            ]
        )
        await callback.message.answer("Grup listesi sonu.", reply_markup=keyboard)
        
    except Exception as e:
        logger.error(f"Grup detayları hatası: {e}")
        await callback.message.edit_text(f"❌ Hata: {str(e)}")

       
# ✅ STATE HANDLER'LAr
# Grup JSON dosyasını işler
@router.message(AdminStates.waiting_for_group_file, F.document)
async def handle_group_file_upload(message: Message, state: FSMContext) -> None:
    """Grup JSON dosyasını işler"""
    try:
        file_id = message.document.file_id
        file_name = message.document.file_name
        
        if not file_name.endswith('.json'):
            await message.answer("❌ Lütfen JSON dosyası gönderin.")
            await state.clear()
            return
        
        logger.info(f"Grup dosyası yükleniyor: {file_name}")
        
        bot = message.bot
        file = await bot.get_file(file_id)
        file_path = config.paths.GROUPS_DIR / "groups_new.json"
        
        await bot.download_file(file.file_path, file_path)
        
        try:
            content = await asyncio.to_thread(lambda: file_path.read_text(encoding='utf-8'))
            groups_data = json.loads(content)
            
            if "groups" not in groups_data or not isinstance(groups_data["groups"], list):
                raise ValueError("Geçersiz grup dosyası formatı")
            
            backup_path = config.paths.GROUPS_DIR / f"groups_backup_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
            original_path = config.paths.GROUPS_DIR / "groups.json"
            
            if await asyncio.to_thread(lambda: original_path.exists()):
                await asyncio.to_thread(lambda: original_path.rename(backup_path))
            
            await asyncio.to_thread(lambda: file_path.rename(original_path))
            await group_manager.refresh_groups()
            
            # ✅ DÜZELTME: Grup sayısını doğru al
            groups_count = len(group_manager.groups)
            
            await message.answer(
                f"✅ Grup dosyası başarıyla güncellendi!\n"
                f"Toplam {groups_count} grup yüklendi.\n"
                f"Yedek: {backup_path.name}"
            )
            
        except Exception as e:
            await message.answer(f"❌ Geçersiz grup dosyası: {str(e)}")
            if await asyncio.to_thread(lambda: file_path.exists()):
                await asyncio.to_thread(lambda: file_path.unlink())
        
    except Exception as e:
        logger.error(f"Grup dosyası yükleme hatası: {e}")
        await message.answer("❌ Dosya işlenirken hata oluştu.")
    finally:
        await state.clear()

 
# JSON yükleme > JSON dışı her şey → iptal
@router.message(AdminStates.waiting_for_group_file)
async def cancel_group_file_wait(message: Message, state: FSMContext):
    """JSON dışında gelen her şeyi yakalayıp işlemi iptal eder"""
    # Sadece JSON dosyası kabul edilir
    if message.document and message.document.file_name.endswith(".json"):
        return  # JSON handler'ı zaten yukarıda bunu işleyecek
    
    # İptal işlemi
    await state.clear()
    await message.answer("❌ Grup yükleme işlemi iptal edildi.")


# Toplu mesaj gönderimini işler
@router.message(AdminStates.waiting_for_broadcast)
async def handle_broadcast_message(message: Message, state: FSMContext) -> None:
    """Toplu mesaj gönderimini işler"""
    try:
        sent_count = 0
        failed_count = 0
        
        for admin_id in config.bot.ADMIN_IDS:  # ✅ config.bot.ADMIN_IDS
            try:
                await message.bot.send_message(
                    admin_id,
                    f"📢 **Toplu Bildirim**\n\n{message.text}"
                )
                sent_count += 1
            except Exception as e:
                logger.error(f"Toplu mesaj gönderilemedi {admin_id}: {e}")
                failed_count += 1
        
        await message.answer(
            f"✅ Toplu mesaj gönderildi!\n"
            f"Başarılı: {sent_count}\n"
            f"Başarısız: {failed_count}"
        )
        
    except Exception as e:
        logger.error(f"Toplu mesaj hatası: {e}")
        await message.answer("❌ Toplu mesaj gönderilemedi.")
    finally:
        await state.clear()


@router.message(AdminStates.waiting_for_group_file)
async def handle_wrong_group_file(message: Message) -> None:
    """Yanlış grup dosyası tipi"""
    await message.answer("❌ Lütfen bir JSON dosyası gönderin.")

@router.message(AdminStates.waiting_for_broadcast)
async def handle_empty_broadcast(message: Message) -> None:
    """Boş broadcast mesajı"""
    await message.answer("❌ Lütfen geçerli bir mesaj yazın.")
   
   
   