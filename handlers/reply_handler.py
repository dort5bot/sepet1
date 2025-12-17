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
                    KeyboardButton(text="Blok"),  # YENİ
                    KeyboardButton(text="Kova"), 
                    KeyboardButton(text="PEX")
                ],
                [
                    KeyboardButton(text="🛑 DUR"),
                    KeyboardButton(text="Js"), 
                    KeyboardButton(text="istatistik"),
                    KeyboardButton(text="Admin")
                ],
            ],
            resize_keyboard=True,
            one_time_keyboard=False,
            input_field_placeholder="Bir işlem seçin veya Excel gönderin...",
        )

async def _show_reply_keyboard(message: Message, title: str = "📋 Hızlı Erişim Menüsü") -> None:
    """
    Ortak reply keyboard gösterici
    """
    keyboard = ReplyKeyboardManager.get_keyboard()
    await message.answer(
        f"{title}\n\nSeçeneklerden birini seçin:",
        reply_markup=keyboard,
    )

async def _send_welcome_message(message: Message) -> None:
    """
    Hoşgeldin mesajı gönderir
    """
    welcome_text = (
        "📊 Excel İşleme Botuna Hoşgeldiniz! - reply \n"
        "version: 17/ 12/ 2025\n"
        "İşlemden önce yada işlem iptali için *🛑 DUR* tıkla \n\n"
        
        "🔄 İşlem Akışı:\n"
        "⚡️ Exceli gruplara ayırmak\n"
        "• Excel'de 1. satırda 'TARİH' ve 'İL' sütunları olmalı\n"
        "• *Kova* tıkla Excel işlemini başlat\n"
        "• Excel dosyasını yükle, gönder \n\n"
        
        "⚡️ şehir isimli dosyaları gruplara göndermek\n"
        "• PEX için dosya adı sadece şehir olmalı (örn: ankara)\n"
        "• *Pex* tıkla işlemi başlat\n"
        "• pdf yada excel dosyasını yükle\n"
        "• İLK dosya TEK , diğerleri topluca yükle, bitince /tamam 'ı tıkla \n\n"
        
        "⚡️ Blok datayı gruplara göndermek\n"
        "1.dosya(ana) 1.satırda TC-İL-TARİH zorunlu yazılacak.\n"
        "2.dosya(tel) 1.satırda TC-TEL zorunlu yazılacak.\n"
        "Yükleme bitince sistem otmotik başlayacak.\n"
        "tüm il bilgisi için> input raporuna bak.\n\n"
        
        "⚡️ Grup işlemleri\n"
        "Yenilemek için 1. json oluştur *js* tıkla \n"
        "grup bilgisi >admin > Grup yönet > grup detay\n"
        ">admin > Grup dosyasını yükle, oluşan json yükle\n"
    )
    await message.answer(welcome_text)
    await _show_reply_keyboard(message)

# ---------------------------------------------------
# MERKEZİ İPTAL FONKSİYONU - TÜM HANDLER'LAR İÇİN - 'ptal eder- temizler
# ---------------------------------------------------
# 🎯 Amaç:
# Kova’ya girdim → vazgeçtim → iptal → PEX başlasın

"""
Kova’ya girdim → vazgeçtim → iptal → PEX başlasın
Yani:
Kova state’i tam temizlenecek
Dosyalar temizlenecek
FSM tamamen sıfırlanacak
Sonrasında PEX’e girince eski state’den EN UFACIK iz kalmayacak


Tüm aktif işlemleri ve state'leri temizle
Tüm handler'lar için ortak iptal fonksiyonu

Args:
clear_files: Dosyaları da temizle (🛑 DUR için True)
"""


async def cancel_all_operations(
    message: Message,
    state: FSMContext,
    clear_files: bool = False
) -> None:
    """
    Tüm aktif işlemleri ve state'leri temizle
    Tüm handler'lar için ortak iptal fonksiyonu
    """
    current_state = await state.get_state()

    # 1️⃣ DOSYALARI TEMİZLE (state varken!)
    if clear_files:
        from handlers.file_handler import clear_all
        await clear_all(message)

    # 2️⃣ FSM KAPAT
    if current_state:
        state_name = current_state.split(":")[-1] if ":" in current_state else current_state

        await state.clear()
        await state.set_data({})  # 🔥 KRİTİK SATIR (TAM BURASI)

        action_text = "İşlemler iptal edildi" + (" ve dosyalar temizlendi" if clear_files else "")
        await message.answer(
            f"❌ **{action_text}**\n\n"
            f"• Aktif durum: `{state_name}`\n"
            f"• Temizlendi: ✅\n\n"
            f"Yeni bir işlem başlatabilirsiniz.",
            reply_markup=ReplyKeyboardManager.get_keyboard()
        )
    else:
        # state yoksa bile ZORLA sıfırla (defansif)
        await state.set_data({})  # 🔒 EMNİYET KEMERİ

        action_text = "Zaten aktif işlem yok" + (" ve dosyalar temizlendi" if clear_files else "")
        await message.answer(
            f"✅ {action_text}.\n"
            f"Yeni işlem başlatmak için menüyü kullanın.",
            reply_markup=ReplyKeyboardManager.get_keyboard()
        )



# ---------------------------------------------------
# KOMUT HANDLER'LARI
# ---------------------------------------------------

@router.message(Command("oku"))
async def cmd_oku(message: Message) -> None:
    """oku komutu - hoşgeldin mesajı ve keyboard"""
    await _send_welcome_message(message)

@router.message(Command("r", "klavye"))
async def cmd_reply_keyboard(message: Message) -> None:
    """r veya klavye - sadece reply keyboard menüsü"""
    await _show_reply_keyboard(message)


# ---------------------------------------------------
# BUTON HANDLER'LARI
# ---------------------------------------------------

@router.message(lambda m: m.text and m.text.lower() == "oku")
async def handle_oku_button(message: Message) -> None:
    """oku butonu - hoşgeldin mesajı"""
    await _send_welcome_message(message)
    
# 🧹 Sadece dosya temizliği yapıldı
@router.message(lambda m: m.text and m.text == "🛑 DUR")
async def handle_stop_button(message: Message, state: FSMContext) -> None:
    """TEST: 🛑 DUR butonu"""
    current_state = await state.get_state()
    await cancel_all_operations(message, state, clear_files=True)
    
   

   

@router.message(lambda m: m.text and m.text == "Blok")
async def handle_block_button(message: Message, state: FSMContext):
    """Blok butonu"""
    from handlers.block_handler import cmd_block
    await cmd_block(message, state)

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

# handle_stats_button fonksiyonunu değiştir
#  Herhangi bir kullanıcı admin paneli ve istatistiklerine erişm önlemek

@router.message(lambda m: m.text and m.text == "istatistik")
async def handle_stats_button(message: Message) -> None:
    """istatistik butonu - sistem istatistiklerini göster"""
    from handlers.admin_handler import is_admin
    
    if not is_admin(message.from_user.id): 
        await message.answer("❌ Bu işlem için admin yetkisi gerekiyor.")
        return
    
    from handlers.admin_handler import _show_admin_stats
    await _show_admin_stats(message)


@router.message(lambda m: m.text and m.text == "Admin")
async def handle_admin_button(message: Message) -> None:
    """Admin butonu - admin panelini açar"""
    from handlers.admin_handler import is_admin
    
    if not is_admin(message.from_user.id):
        await message.answer("❌ Bu işlem için admin yetkisi gerekiyor.")
        return
    
    from handlers.admin_handler import get_admin_keyboard
    keyboard = get_admin_keyboard()
    await message.answer("👑 **Admin Paneli**\n\nAşağıdaki seçeneklerden birini seçin:", reply_markup=keyboard)
    
    
    