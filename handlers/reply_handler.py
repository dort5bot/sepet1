"""
Reply Keyboard Handler - Tüm işlemler burada merkezileşti
Kullanıcı dostu arayüz, hızlı erişim ve iptal işlemleri

18-11-2025
merkesi iptal butonu burda
Komutlar: /dur, /stop, /cancel, /iptal

Reply Keyboard → Kullanıcı dostu arayüz:
Temizle → /clear
Kova → /process
tek → /tek
JSON yap → /js
Komutlar → /dar komutunu ekle, tümünü bu maile atar)
"""

# handlers/reply_handler.py


from aiogram import Router
from aiogram.types import Message, ReplyKeyboardMarkup, KeyboardButton
from aiogram.filters import Command
from aiogram.fsm.context import FSMContext

# Handler loader uyumlu router tanımı
router = Router(name="reply_keyboard")

class ReplyKeyboardManager:
    """
    Reply keyboard yönetimi - Singleton pattern
    """
    _instance: ReplyKeyboardMarkup = None
    
    @classmethod
    def get_keyboard(cls) -> ReplyKeyboardMarkup:
        """Tekil keyboard örneğini döndürür"""
        if cls._instance is None:
            cls._instance = cls._create_keyboard()
        return cls._instance
    
    @staticmethod
    def _create_keyboard() -> ReplyKeyboardMarkup:
        """Keyboard oluşturur"""
        return ReplyKeyboardMarkup(
            keyboard=[
                [
                    KeyboardButton(text="oku"), 
                    KeyboardButton(text="Temizle"), 
                    KeyboardButton(text="Kova"), 
                    KeyboardButton(text="PEX")
                ],
                [
                    KeyboardButton(text="🛑 DUR"),
                    KeyboardButton(text="Js"), 
                    #KeyboardButton(text="Komutlar"),
                    KeyboardButton(text="istatistik"),    #Admin işlemi
                    KeyboardButton(text="Admin")                #Admin işlemi
                ],
            ],
            resize_keyboard=True,
            one_time_keyboard=False,
            input_field_placeholder="Bir işlem seçin veya Excel gönderin...",
        )

async def _show_reply_keyboard(message: Message, title: str) -> None:
    """
    Ortak reply keyboard gösterici
    """
    keyboard = ReplyKeyboardManager.get_keyboard()
    await message.answer(
        f"{title}\n\nSeçeneklerden birini seçin veya Excel dosyası gönderin:",
        reply_markup=keyboard,
    )

async def _send_welcome_message(message: Message) -> None:
    """
    Hoşgeldin mesajı gönderir
    """
    welcome_text = (
        "📊 Excel İşleme Botuna Hoşgeldiniz!\n"
        "version: 17/11/2025\n"
        "her işlem önce **Temizle** ve DUR butonuna tıkla\n\n"
        "🔄 **İşlem Akışı:**\n"
        "exceli gruplara ayır"
        "• Excel'de 1. satırda 'TARİH' ve 'İL' sütunları olmalı\n"
        "1️⃣ **Kova** ile Excel işlemini başlat\n"
        "2️⃣  excel dosyasını yükle\n"
        "3️⃣ **🛑 DUR** ile istediğin zaman iptal et\n\n"
        "şehir isimli dosyaları gruplara gönderme\n"
        "• PEX için dosya adı küçük harf (örn: ankara.pdf)\n"
        "1️⃣ **Pex** ile işlemi başlat\n"
        "2️⃣ pdf yada excel dosyasını yükle(kars.xls)\n"
        "3️⃣ **🛑 DUR** ile istediğin zaman iptal et\n\n"
        "Grup dosyasını yenilemek için /js komutu> admin \n"
    )
    await message.answer(welcome_text)
    await _show_reply_keyboard(message, "📋 Hızlı Erişim Menüsü")

# ---------------------------------------------------
# MERKEZİ İPTAL FONKSİYONU - TÜM HANDLER'LAR İÇİN
# ---------------------------------------------------

async def cancel_all_operations(message: Message, state: FSMContext) -> None:
    """
    Tüm aktif işlemleri ve state'leri temizle
    Tüm handler'lar için ortak iptal fonksiyonu
    """
    current_state = await state.get_state()
    
    if current_state:
        # State adını daha açıklayıcı göster
        state_name = current_state.split(":")[-1] if ":" in current_state else current_state
        await state.clear()
        
        await message.answer(
            f"🛑 **Tüm işlemler İptal Edildi**\n\n"
            f"• Aktif durum: `{state_name}`\n"
            f"• Temizlendi: ✅\n\n"
            f"Yeni bir işlem başlatabilirsiniz.",
            reply_markup=ReplyKeyboardManager.get_keyboard()
        )
    else:
        await message.answer(
            "✅ Zaten aktif işlem yok.\n"
            "Yeni işlem başlatmak için menüyü kullanın.",
            reply_markup=ReplyKeyboardManager.get_keyboard()
        )

# ---------------------------------------------------
# KOMUT HANDLER'LARI
# ---------------------------------------------------

@router.message(Command("oku"))
async def cmd_oku(message: Message) -> None:
    """/oku komutu - hoşgeldin mesajı ve keyboard"""
    await _send_welcome_message(message)

@router.message(Command("r", "klavye"))
async def cmd_reply_keyboard(message: Message) -> None:
    """/r veya /klavye - sadece reply keyboard menüsü"""
    await _show_reply_keyboard(message, "📋 Hızlı Erişim Menüsü")

@router.message(Command("dur", "stop", "cancel", "iptal"))
async def cmd_cancel(message: Message, state: FSMContext) -> None:
    """Tüm iptal komutları - merkezi iptal"""
    await cancel_all_operations(message, state)

# ---------------------------------------------------
#// BUTON HANDLER'LARI
#// ---------------------------------------------------

@router.message(lambda m: m.text and m.text.lower() == "oku")
async def handle_oku_button(message: Message) -> None:
    """oku butonu - hoşgeldin mesajı"""
    await _send_welcome_message(message)



# Temizle = "Sadece manuel dosya temizliği yapar, aktif işleme dokunmaz"
# Asla temizlenmez: logs/, groups/, CONFIG dosyaları
@router.message(lambda m: m.text and m.text == "Temizle")
async def handle_clear_button(message: Message) -> None:
    """Temizle butonu - sadece dosya temizliği"""
    from handlers.file_handler import clear_all
    
    await message.answer("🧹 Sadece dosya temizliği yapıldı...")
    await clear_all(message)

# 🛑 DUR = İşlem durdurur + State(geçici dosyalar) temizler
@router.message(lambda m: m.text and m.text == "🛑 DUR")
async def handle_stop_button(message: Message, state: FSMContext) -> None:
    """🛑 DUR butonu - tüm işlemleri iptal et + dosyaları temizle"""
    # Önce state'i temizle
    await cancel_all_operations(message, state)
    
    # Sonra dosyaları temizle
    from handlers.file_handler import clear_all
    await clear_all(message)



@router.message(lambda m: m.text and m.text == "Kova")
async def handle_kova_button(message: Message, state: FSMContext) -> None:
    """Kova butonu - işleme başlatma"""
    from handlers.kova_handler import cmd_process
    await cmd_process(message, state)

@router.message(lambda m: m.text and m.text == "PEX")
async def handle_pex_button(message: Message, state: FSMContext) -> None:
    """PEX butonu - dosya dağıtımı"""
    from handlers.pex_handler import cmd_pex
    await cmd_pex(message, state)


@router.message(lambda m: m.text and m.text == "Js")
async def handle_json_button(message: Message, state: FSMContext) -> None:
    """Js butonu - JSON oluşturma"""
    from handlers.json_handler import handle_json_command
    await handle_json_command(message, state)




# Grup Detayları butonu - grup bilgilerini göster
"""
@router.message(lambda m: m.text and m.text == "Grup Detay")
async def handle_group_details_button(message: Message) -> None:
    from handlers.admin_handler import _show_group_details
    
    # Admin kontrolü yap
    from handlers.admin_handler import is_admin
    if not is_admin(message.from_user.id):
        await message.answer("❌ Bu işlem için admin yetkisi gerekiyor.")
        return
    
    # Grup detaylarını göster
    await _show_group_details(message)
"""


@router.message(lambda m: m.text and m.text == "istatistik")
async def handle_stats_button(message: Message) -> None:
    """istatistik butonu - sistem istatistiklerini göster"""
    from handlers.admin_handler import _show_admin_stats
    
    # Admin kontrolü yap
    from handlers.admin_handler import is_admin
    if not is_admin(message.from_user.id):
        await message.answer("❌ Bu işlem için admin yetkisi gerekiyor.")
        return
    
    # İstatistikleri göster
    await _show_admin_stats(message)
 

@router.message(lambda m: m.text and m.text == "Admin")
async def handle_admin_button(message: Message) -> None:
    """Admin butonu - admin panelini açar"""
    # Admin kontrolü yap
    from handlers.admin_handler import is_admin
    if not is_admin(message.from_user.id):
        await message.answer("❌ Bu işlem için admin yetkisi gerekiyor.")
        return
    
    # Admin paneli klavyesini göster
    from handlers.admin_handler import get_admin_keyboard
    keyboard = get_admin_keyboard()
    
    await message.answer(
        "👑 **Admin Paneli**\n\nAşağıdaki seçeneklerden birini seçin:", 
        reply_markup=keyboard
    )

  
@router.message(lambda m: m.text and m.text == "Komutlar")
async def handle_commands_button(message: Message) -> None:
    """Komutlar butonu - komut listesi"""
    from handlers.dar_handler import scan_handlers_for_commands
    
    scanned = scan_handlers_for_commands()
    if not scanned:
        await message.answer("❌ Komut bulunamadı.")
        return
    
    lines = [f"{cmd} → {desc}" for cmd, desc in sorted(scanned.items())]
    text = "\n".join(lines)
    await message.answer(f"<pre>{text}</pre>", parse_mode="HTML")