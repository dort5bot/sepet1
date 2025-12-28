import json
import re
from pathlib import Path
from aiogram import Router, F
from aiogram.types import Message, ReplyKeyboardMarkup, KeyboardButton
from aiogram.filters import StateFilter
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup

router = Router()
DOSYA_ADI = Path("data/notlar_v2.json")

# 1. TEK BİR STATE GRUBU TANIMI
class NotSistemi(StatesGroup):
    ana_menu = State()        # Notlar ana ekranı
    not_bekliyor = State()    # Not içeriği yazılırken
    silme_bekliyor = State()  # Silinecek buton seçilirken

# --- Veri Yönetimi ---
def yukle_veriler():
    if DOSYA_ADI.exists():
        try:
            with open(DOSYA_ADI, "r", encoding="utf-8") as f: return json.load(f)
        except: return {}
    return {}

def kaydet_veriler(veriler):
    DOSYA_ADI.parent.mkdir(parents=True, exist_ok=True)
    with open(DOSYA_ADI, "w", encoding="utf-8") as f:
        json.dump(veriler, f, ensure_ascii=False, indent=2)

def baslik_olustur(metin, mevcut_notlar):
    temiz = re.sub(r'\W+', '', metin)
    baslik = temiz[:8].lower() or "not"
    orijinal, sayac = baslik, 1
    while baslik in mevcut_notlar:
        suffix = str(sayac)
        baslik = f"{orijinal[:(8-len(suffix))]}{suffix}"
        sayac += 1
    return baslik

def not_menu_klavyesi(user_id):
    veriler = yukle_veriler()
    user_notes = veriler.get(str(user_id), {})
    kb = [[KeyboardButton(text="ekle"), KeyboardButton(text="sil"), 
           KeyboardButton(text="geri"), KeyboardButton(text="Ev")]]
    basliklar = list(user_notes.keys())
    for i in range(0, len(basliklar), 4):
        kb.append([KeyboardButton(text=b) for b in basliklar[i:i+4]])
    return ReplyKeyboardMarkup(keyboard=kb, resize_keyboard=True)


# --- HANDLERLAR ---

# Giriş: "notlar" butonuna basıldığında
@router.message(F.text == "notlar")
async def notlar_ana(message: Message, state: FSMContext):
    await state.set_state(NotSistemi.ana_menu) # State'i aktif et
    uid = str(message.from_user.id)
    await message.answer("📝 Not Defteri", reply_markup=not_menu_klavyesi(uid))

# İptal/Geri İşlemleri
@router.message(NotSistemi.not_bekliyor, F.text.casefold() == "geri")
@router.message(NotSistemi.silme_bekliyor, F.text.casefold() == "geri")
async def islem_iptal(message: Message, state: FSMContext):
    await notlar_ana(message, state) # Tekrar ana menüye ve state'ine döner

# EKLEME BAŞLAT
@router.message(NotSistemi.ana_menu, F.text == "ekle")
async def not_ekle_baslat(message: Message, state: FSMContext):
    await state.set_state(NotSistemi.not_bekliyor)
    await message.answer("📝 Notunuzu yazın:", 
                         reply_markup=ReplyKeyboardMarkup(keyboard=[[KeyboardButton(text="geri")]], resize_keyboard=True))

# KAYDETME
@router.message(NotSistemi.not_bekliyor)
async def not_kaydet(message: Message, state: FSMContext):
    uid, icerik = str(message.from_user.id), message.text.strip()
    veriler = yukle_veriler()
    veriler.setdefault(uid, {})
    baslik = baslik_olustur(icerik, veriler[uid])
    veriler[uid][baslik] = icerik
    kaydet_veriler(veriler)
    await notlar_ana(message, state) # Kayıttan sonra menüye dön

# SİLME MODU
@router.message(NotSistemi.ana_menu, F.text == "sil")
async def sil_modu_aktif(message: Message, state: FSMContext):
    await state.set_state(NotSistemi.silme_bekliyor)
    await message.answer("🗑 Silinecek notun butonuna tıklayın:", 
                         reply_markup=not_menu_klavyesi(message.from_user.id))

# SİLME GERÇEKLEŞTİRME
@router.message(NotSistemi.silme_bekliyor)
async def not_gercekten_sil(message: Message, state: FSMContext):
    if message.text == "geri": return await notlar_ana(message, state)
    uid, silinecek = str(message.from_user.id), message.text
    veriler = yukle_veriler()
    if uid in veriler and silinecek in veriler[uid]:
        del veriler[uid][silinecek]
        kaydet_veriler(veriler)
        await message.answer(f"❌ '{silinecek}' silindi.")
    await notlar_ana(message, state)

# GÖRÜNTÜLEME (En sona koyduk ki ekle/sil/geri butonlarını yutmasın)

@router.message(
    NotSistemi.ana_menu, # 2. Sadece notlar menüsündeyken mesajları dinle
    F.text & ~F.text.in_(["Ev", "ekle", "sil", "geri", "notlar"])
)

# @router.message(NotSistemi.ana_menu, F.text)
async def not_goster(message: Message, state: FSMContext):
    uid, text = str(message.from_user.id), message.text
    veriler = yukle_veriler()
    user_notes = veriler.get(uid, {})
    if text in user_notes:
        await message.answer(f"📌 {text.upper()}\n---\n{user_notes[text]}")
    else:
        return # Not değilse diğer handlerlara (Kova, Pex vb.) pasla