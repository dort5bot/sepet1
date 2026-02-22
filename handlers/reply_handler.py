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

import asyncio
RUNNING_TASKS: dict[int, asyncio.Task] = {}



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
                    KeyboardButton(text="Kova"), 
                    KeyboardButton(text="PEX"),
                    KeyboardButton(text="Sgk")  
                ],
                [
                    KeyboardButton(text="🛑 DUR"),
                    KeyboardButton(text="Js"), 
                    KeyboardButton(text="notlar"), #KeyboardButton(text="istatistik")
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
        # f"{title}\n\nSeçeneklerden birini seçin:",    >> başlık kod içinde: 📋 Hızlı
        f"🔷Bir işlem seçin:🔷",
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
        "⚡️ kova: Exceli gruplara ayırmak\n"
        "• Zorunlu: Excel'de 1.satırda 'TARİH', 'İL' sütunları olmalı\n"
        "• *Kova* tıkla Excel işlemini başlat\n"
        "• Excel dosyasını yükle, gönder \n\n"
        
        "⚡️ PEX: şehir isimli dosyaları gruplara göndermek\n"
        "• zorunlu: dosya adı şehir olmalı (van, van-tuşpa)\n"
        "• *Pex* tıkla işlemi başlat\n"
        "• pdf yada excel dosyasını yükle\n"
        "• İLK dosya TEK , diğerleri topluca yükle, bitince /tamam 'ı tıkla \n\n"
        
        "⚡️ Sgk: Ssk datasını gruplara gönderir\n"
        "• Dosya-1(ana) 1.satırda TC-İL-TARİH zorunlu yazılacak.\n"
        "• Dosya-2 (tel) 1.satırda TC-TEL zorunlu yazılacak.\n"
        "• Yükleme bitince sistem otmotik başlayacak.\n\n"
        " ❗️ dosya yüklenir ve işlem başlarsa, durdurulamaz"

    )
    await message.answer(welcome_text)
    await _show_reply_keyboard(message)

# ---------------------------------------------------
# MERKEZİ İPTAL FONKSİYONU - TÜM HANDLER'LAR İÇİN - 'ptal eder- temizler
# ---------------------------------------------------
# 🎯 Amaç:
# Kova’ya girdim → vazgeçtim → iptal → PEX başlasın

"""
(FSM-merkezli iptal)
Kova’ya girdim → vazgeçtim → iptal → PEX başlasın
Sonrasında PEX’e girince eski state’den EN UFACIK iz kalmayacak

FSM kesin ve doğru biçimde temizlenir
Dosyalar state varken temizlenir (en kritik nokta)
Mail / rapor / FSM zinciri devam etmez
Kullanıcıya doğru ve gerçekçi mesaj gider
Tek merkez (reply / cancel handler)
❌ İş fiziksel olarak çalışır, bu ZOR çok ek saçma kod gerektirir

Args:
clear_files: Dosyaları da temizle (🛑 DUR için True)
"""


async def cancel_all_operations(
    message: Message,
    state: FSMContext,
    clear_files: bool = False
) -> None:
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
            f"• 🧹 Temizlik yapıldı: ✅\n\n"
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
    

# @router.message(lambda m: m.text and m.text == "🛑 DUR")
@router.message(lambda m: m.text in {"🛑 DUR", "Ev"})
async def handle_stop_button(message: Message, state: FSMContext) -> None:
    """🛑 DUR butonu: işlemleri durdurur, merkeze döner"""
    # current_state = await state.get_state()
    await cancel_all_operations(message, state, clear_files=True)
    


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

@router.message(lambda m: m.text and m.text == "Sgk")
async def handle_sgk_button(message: Message, state: FSMContext):
    """sgk butonu"""
    from handlers.sgk_handler import cmd_sgk
    await cmd_sgk(message, state)



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


@router.message(lambda m: m.text and m.text == "notlar")
async def handle_not_button(message: Message, state: FSMContext) -> None:
    from handlers.not_handler import notlar_ana
    await notlar_ana(message, state)




@router.message(lambda m: m.text and m.text == "Admin")
async def handle_admin_button(message: Message) -> None:
    """Admin butonu - admin panelini açar"""
    from handlers.admin_handler import is_admin
    
    if not is_admin(message.from_user.id):
        await message.answer("❌ Bu işlem için admin yetkisi gerekiyor.")
        return
    
    from handlers.admin_handler import get_admin_keyboard
    keyboard = get_admin_keyboard()
    text = (
        "👑 **Admin Paneli**\n\n"
        "⚡️ **Grup bilgisi işlemleri**\n"
        "➡️ Grup yönetimi\n"
        "- grup detay bilgisi için\n\n"
        "➡️ Grup Dosyası Yükle\n"
        "- mevcut grup bilgisini güncellemek için\n"
        "1 Yenilemek için **1. json oluştur (js)** tıkla\n"
        "2 Grup dosyasını yükle  →  json'u yükle"
    )
    await message.answer(text, reply_markup=keyboard)
        
    