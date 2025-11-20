# handlers/json_handler.py
"""
JSON Handler Module
Excel dosyalarını JSON formatına dönüştürme işlemleri

17/11/2025
"""
import os
import tempfile
import logging
from aiogram import Router, F
from aiogram.types import Message, BufferedInputFile
from aiogram.filters import Command
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from utils.json_processing import process_excel_to_json

# Router tanımı - handler_loader uyumlu
router = Router(name="json_processor")

# Logger tanımı - handler_loader ile uyumlu
logger = logging.getLogger(__name__)

class JsonProcessingState(StatesGroup):
    """JSON işleme state'leri"""
    waiting_for_excel = State()

@router.message(Command("js"))
async def handle_json_command(message: Message, state: FSMContext):
    """
    /js komutunu işler ve Excel dosyası bekler
    """
    logger.debug(f"JSON komutu tetiklendi - Kullanıcı: {message.from_user.id}")
    await message.answer("📊 Lütfen işlemek istediğiniz Excel dosyasını gönderin...")
    await state.set_state(JsonProcessingState.waiting_for_excel)

@router.message(JsonProcessingState.waiting_for_excel, F.document)
async def handle_excel_file(message: Message, state: FSMContext):
    """
    Excel dosyasını işler ve JSON'a dönüştürür
    """
    # İPTAL KONTROLÜ - EKLENDİ
    if message.text and message.text == "🛑 DUR":
        from handlers.reply_handler import cancel_all_operations
        await cancel_all_operations(message, state)
        return
        
    user_id = message.from_user.id
    file_name = message.document.file_name
    
    logger.debug(f"Excel dosyası alındı - Kullanıcı: {user_id}, Dosya: {file_name}")

    # Dosya formatı kontrolü
    if not message.document.file_name.endswith(('.xlsx', '.xls')):
        await message.answer("❌ Sadece Excel dosyaları (.xlsx, .xls) desteklenmektedir.")
        await state.clear()
        return

    temp_file_path = None
    try:
        # Dosyayı indir
        file_info = await message.bot.get_file(message.document.file_id)
        downloaded_file = await message.bot.download_file(file_info.file_path)

        # Geçici dosya oluştur
        with tempfile.NamedTemporaryFile(delete=False, suffix='.xlsx') as tmp_file:
            tmp_file.write(downloaded_file.read())
            temp_file_path = tmp_file.name

        # İşlemi başlat
        await message.answer("⏳ Excel dosyası işleniyor...")

        # JSON işleme
        json_file_path = await process_excel_to_json(temp_file_path)

        if json_file_path and os.path.exists(json_file_path):
            # JSON dosyasını oku ve gönder
            with open(json_file_path, 'rb') as json_file:
                json_data = json_file.read()
            
            # JSON dosyasını gönder
            input_file = BufferedInputFile(json_data, filename="groups.json")
            await message.answer_document(input_file, caption="✅ Grup verileri başarıyla oluşturuldu!")
            
            logger.info(f"JSON işleme başarılı - Kullanıcı: {user_id}, Dosya: {file_name}")
            
        else:
            await message.answer("❌ JSON dosyası oluşturulamadı.")
            logger.warning(f"JSON oluşturma başarısız - Kullanıcı: {user_id}")

    except Exception as e:
        logger.error(f"JSON işleme hatası - Kullanıcı: {user_id}: {str(e)}", exc_info=True)
        await message.answer(f"❌ Hata oluştu: {str(e)}")
        
    finally:
        # Temizlik işlemleri
        await _cleanup_temp_files(temp_file_path)
        await state.clear()

@router.message(JsonProcessingState.waiting_for_excel)
async def handle_wrong_file_type(message: Message, state: FSMContext):
    """
    Excel dosyası dışında bir şey gönderilirse
    """
    # İPTAL KONTROLÜ - EKLENDİ
    if message.text and message.text == "🛑 DUR":
        from handlers.reply_handler import cancel_all_operations
        await cancel_all_operations(message, state)
        return
        
    logger.debug(f"Geçersiz dosya tipi - Kullanıcı: {message.from_user.id}")
    await message.answer("❌ Lütfen sadece Excel dosyası (.xlsx, .xls) gönderin.")
    await state.clear()

async def _cleanup_temp_files(file_path: str):
    """
    Geçici dosyaları temizler
    """
    try:
        if file_path and os.path.exists(file_path):
            os.unlink(file_path)
            logger.debug(f"Geçici dosya silindi: {file_path}")
    except Exception as e:
        logger.warning(f"Geçici dosya silinemedi {file_path}: {e}")
        