# [file name]: sgk_handler.py
# [file content begin]
"""
Sgk bazlı Excel işleme handler'ı (Basitleştirilmiş)
AKIŞ:
1) Ana Excel alınır (ham: İL-TARİH-TC)
2) Veri Excel alınır (tel:TC-TEL)
3-1) TC MERGE yapılır (tek dosya: İL-TARİH-TC-GSM)
3-2) İL bilgisi tek satırdan her satıra dağıtılır
4) MERGED dosya excel_process modülü ile işlenir
Ham Excel → Tel Excel → TC Merge → Excel Process → Sonuç (satır halinde: İL-TARİH-TC-GSM)


"""

import asyncio
from pathlib import Path
import tempfile

from aiogram import Router, F
from aiogram.types import Message
from aiogram.filters import Command
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup

from config import config
from utils.tc_merger import build_merged_excel, process_city_il
from utils.excel_process import process_excel_task
from utils.reporter import generate_processing_report
from utils.mailer import send_email
from utils.logger import logger

router = Router(name="Sgk_processor")


# ===================== FSM =====================

class BlockProcessingStates(StatesGroup):
    waiting_for_main = State()   # Ham dosya (TC-İL-TARİH)
    waiting_for_data = State()   # Tel dosyası (TC-TEL)


# ===================== COMMAND =====================

@router.message(Command("sgk"))
async def cmd_sgk(message: Message, state: FSMContext):
    """Sgk işlemleri başlat"""
    await state.set_state(BlockProcessingStates.waiting_for_main)
    await message.answer(
        "📄 **(SGK) data İşlemleri**\n\n"
        "2 dosyada TC=TEL eşleştirir, gruplara atar\n"
        "1.satıra ZORUNLU başlıklar yazılmalıdır\n"
        " ❗️ Sıra ÖNEMLİDİR\n\n"
        "1️⃣  Dosya-1 Ham dosya (TC-İL-TARİH) gönder\n"
        "2️⃣  Dosya-2 Tel dosyası (TC-TEL) gönder\n"
        "🛑 İptal için bas: DUR"
    )


# ===================== HAM DOSYA -  İlk Excel dosyasını işle =====================

@router.message(BlockProcessingStates.waiting_for_main, F.document)
async def handle_main_excel(message: Message, state: FSMContext):
    """İlk Excel dosyasını işle (ham)"""
    if not message.document.file_name.endswith((".xlsx", ".xls")):
        await message.answer("❌ Sadece Excel dosyaları (.xlsx, .xls) kabul edilir")
        return

    try:
        # Dosyayı indir
        file_info = await message.bot.get_file(message.document.file_id)
        tmp = tempfile.NamedTemporaryFile(delete=False, suffix=".xlsx")
        tmp.close()
        await message.bot.download_file(file_info.file_path, tmp.name)
        
        # ✅ İLK DOSYA ADINI KAYDET
        main_excel_name = message.document.file_name
        
        await state.update_data({
            "main_excel": Path(tmp.name),
            "main_excel_name": main_excel_name  # ✅ Dosya adını kaydet
        })
        await state.set_state(BlockProcessingStates.waiting_for_data)
        
        await message.answer(
            f"✅ **İlk dosya alındı: {main_excel_name}**\n\n"
            "📄 **İkinci Excel dosyasını gönderin**\n"
            "(TC - TEL sütunları olmalı)"
        )
        
    except Exception as e:
        logger.error(f"Dosya indirme hatası: {e}")
        await message.answer("❌ Dosya indirilemedi")
        await state.clear()

# ===================== TEL DOSYASI → MERGE → İŞLEME =====================

@router.message(BlockProcessingStates.waiting_for_data, F.document)
async def handle_data_excel(message: Message, state: FSMContext):
    """İkinci Excel dosyasını işle ve süreci başlat"""
    if not message.document.file_name.endswith((".xlsx", ".xls")):
        await message.answer("❌ Sadece Excel dosyaları (.xlsx, .xls) kabul edilir")
        return

    data = await state.get_data()
    main_excel = data.get("main_excel")
    main_excel_name = data.get("main_excel_name", "Bilinmeyen Dosya")
    
    
    if not main_excel or not main_excel.exists():
        await message.answer("❌ İlk dosya bulunamadı, işlem iptal edildi")
        await state.clear()
        return

    try:
        # İkinci dosyayı indir
        file_info = await message.bot.get_file(message.document.file_id)
        tmp = tempfile.NamedTemporaryFile(delete=False, suffix=".xlsx")
        tmp.close()
        await message.bot.download_file(file_info.file_path, tmp.name)
        data_excel = Path(tmp.name)

        await message.answer("🔄 **İşlem başlatıldı...**")

        # 1. TC Merge işlemi
        await message.answer("1️⃣ TC eşleştirmesi yapılıyor...")
        
        merge_path = config.paths.TEMP_DIR / "sgk1.xlsx"
        final_merged = await asyncio.get_running_loop().run_in_executor(
            None,
            build_merged_excel,
            main_excel,
            data_excel,
            merge_path
        )

        # 2. City/İL düzenleme
        await message.answer("2️⃣ Şehir/İL düzenlemesi yapılıyor...")
        
        final_path = config.paths.TEMP_DIR / "sgk.xlsx"
        await asyncio.get_running_loop().run_in_executor(
            None,
            process_city_il,
            final_merged,
            final_path
        )

        # 3. Excel işleme (excel_process modülü)
        await message.answer("3️⃣ Excel işleme ve mail gönderimi başlatılıyor...")

        # ✅ main_excel_name'i parametre olarak gönder
        processing_result = await process_excel_task(
            final_path, 
            user_id=message.from_user.id,
            main_excel_name=main_excel_name  # Bu parametreyi ekleyin
        )

        # ilk dosya adını İşlem sonucuna ekle
        processing_result['main_excel_name'] = main_excel_name  # <-- buraya ekle



        # 4. Sonuç raporu
        if processing_result.get("success", False):
            # Telegram mesajı için rapor (detaylı)
            report_text = generate_processing_report(processing_result, for_internal_message=True)

       
            await message.answer(f"✅ **İşlem Tamamlandı**\n\n{report_text}")
        else:
            error_msg = processing_result.get("error", "Bilinmeyen hata")
            await message.answer(f"❌ **İşlem Başarısız**\n\nHata: {error_msg}")

    except Exception as e:
        logger.error(f"Block işlem hatası: {e}", exc_info=True)
        await message.answer(f"❌ **İşlem Hatası**\n\n{str(e)}")

    finally:
        # Temizlik
        try:
            for path in [main_excel, data_excel]:
                if path and path.exists():
                    path.unlink(missing_ok=True)
            
            temp_files = ["sgk1.xlsx", "sgk.xlsx"]
            for file_name in temp_files:
                file_path = config.paths.TEMP_DIR / file_name
                if file_path.exists():
                    file_path.unlink(missing_ok=True)
        except Exception as cleanup_error:
            logger.warning(f"Geçici dosya temizleme hatası: {cleanup_error}")
        
        await state.clear()


# ===================== DURUM SORGULAMA =====================

@router.message(Command("cancel"))
async def cmd_cancel(message: Message, state: FSMContext):
    """Mevcut işlemi iptal et"""
    current_state = await state.get_state()
    if current_state:
        await state.clear()
        await message.answer("❌ İşlem iptal edildi")
    else:
        await message.answer("❌ Aktif bir işlem yok")
