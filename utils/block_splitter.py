# utils/block_splitter.py
"""
Blok bazlı Excel bölücü - Şehir bloklarını bulur ve gruplara ayırır.
"""

import unicodedata
import asyncio
from pathlib import Path
from typing import Dict, List, Any, Tuple
from datetime import datetime, date

from concurrent.futures import ThreadPoolExecutor
import re

import xlsxwriter
from openpyxl import load_workbook

from utils.group_manager import group_manager
from utils.file_namer import generate_output_filename
from utils.logger import logger
from config import config






class BlockExcelSplitter:
    """Blok bazlı Excel splitter - Şehir bloklarını bulur"""
    _CITY_SET: set[str] | None = None
    
    def __init__(self, input_path: str, headers: List[str]):
        self.input_path = input_path
        self.headers = headers
        self._executor = ThreadPoolExecutor(max_workers=2)
        

    @staticmethod
    def normalize_turkish(text) -> str:
        if not text or not isinstance(text, str):
            return ""

        # 1) Unicode ayrıştır
        text = unicodedata.normalize("NFKD", text)

        # 2) COMBINING DOT ABOVE (İ̇) temizle
        text = text.replace("\u0307", "")

        # 3) Büyük harf (Python burada artık düzgün çalışır)
        return text.strip().upper()





    @classmethod
    def _get_city_set(cls) -> set[str]:
        if cls._CITY_SET is None:
            raw_cities = [
                "Adana", "Adıyaman", "Afyon", "Ağrı", "Amasya", "Ankara",
                "Antalya", "Artvin", "Aydın", "Balıkesir", "Bilecik", "Bingöl",
                "Bitlis", "Bolu", "Burdur", "Bursa", "Çanakkale", "Çankırı",
                "Çorum", "Denizli", "Diyarbakır", "Edirne", "Elazığ", "Erzincan",
                "Erzurum", "Eskişehir", "Gaziantep", "Giresun", "Gümüşhane",
                "Hakkari", "Hatay", "Isparta", "İçel", "İstanbul", "İzmir",
                "Kars", "Kastamonu", "Kayseri", "Kırklareli", "Kırşehir",
                "Kocaeli", "Konya", "Kütahya", "Malatya", "Manisa",
                "Kahramanmaraş", "Mardin", "Muğla", "Muş", "Nevşehir",
                "Niğde", "Ordu", "Rize", "Sakarya", "Samsun", "Siirt",
                "Sinop", "Sivas", "Tekirdağ", "Tokat", "Trabzon", "Tunceli",
                "Şanlıurfa", "Uşak", "Van", "Yozgat", "Zonguldak", "Aksaray",
                "Bayburt", "Karaman", "Kırıkkale", "Batman", "Şırnak",
                "Bartın", "Ardahan", "Iğdır", "Yalova", "Karabük",
                "Kilis", "Osmaniye", "Düzce"
            ]

            cls._CITY_SET = {cls.normalize_turkish(c) for c in raw_cities}

        return cls._CITY_SET




        
        # Hücrede sadece şehir adı mı var kontrol eder.
        # 'SİVAS DEVLET HASTANESİ' gibi ifadeleri eler.
           

    def _is_city_row(self, cell_value) -> tuple[bool, str]:
        value = self.normalize_turkish(cell_value)
        if not value:
            return False, ""

        city_set = self._get_city_set()

        if value in city_set:
            return True, value

        if value.endswith(" İLİ"):
            city = value[:-4]
        elif value.endswith(" İL"):
            city = value[:-3]
        else:
            return False, ""

        return (city in city_set), city if city in city_set else ""




# utils/block_splitter.py'ye debug log ekle:


    def _find_city_blocks(self, ws) -> List[Dict[str, Any]]:
        blocks = []
        current_city = None
        start_row = None
        
        logger.debug(f"📊 Excel satır sayısı: {ws.max_row}")
        
        # Başlık satırından sonra başla
        for row in range(2, ws.max_row + 1):
            # İL sütunundaki değeri al (varsayılan B sütunu)
            city_cell = ws.cell(row=row, column=2).value
            
            # DEBUG: Her satırı kontrol et
            if row % 100 == 0:  # Her 100 satırda bir log
                logger.debug(f"Satır {row}: '{city_cell}'")
            
            is_city, city_name = self._is_city_row(city_cell)
            
            if is_city:
                logger.info(f"📍 Şehir bulundu satır {row}: {city_name}")
                
                # Önceki bloğu kapat
                if current_city and start_row:
                    blocks.append({
                        "city": current_city,
                        "start_row": start_row,
                        "end_row": row - 1,
                        "max_column": ws.max_column
                    })
                    logger.info(f"📦 Blok kapatıldı: {current_city} ({start_row}-{row-1})")
                
                # Yeni blok başlat
                current_city = city_name
                start_row = row + 1  # Şehir satırından sonraki satır
                logger.info(f"🆕 Yeni blok başlatıldı: {city_name} ({start_row}'den)")
        
        # Son bloğu kapat
        if current_city and start_row:
            blocks.append({
                "city": current_city,
                "start_row": start_row,
                "end_row": ws.max_row,
                "max_column": ws.max_column
            })
            logger.info(f"📦 Son blok kapatıldı: {current_city} ({start_row}-{ws.max_row})")
        
        return blocks


    # Blokları işler ve gruplara ayırır
    async def process_blocks(self) -> Dict[str, Any]:
        try:
            await group_manager._ensure_initialized()

            # Excel'i yükle
            def load_workbook_sync():
                return load_workbook(self.input_path, read_only=True)

            loop = asyncio.get_running_loop()
            wb = await loop.run_in_executor(self._executor, load_workbook_sync)
            ws = wb.active

            # Blokları bul
            blocks = await loop.run_in_executor(
                self._executor, self._find_city_blocks, ws
            )

            output_files = {}
            total_rows = 0

            for block in blocks:
                city = block["city"]

                # Şehrin bağlı olduğu grupları al
                group_ids = await group_manager.get_groups_for_city(city)

                # Eğer şehir hiçbir gruba bağlı değilse → grup_0
                if not group_ids:
                    logger.info(f"📦 Şehir gruba bağlı değil, grup_0’a alındı: {city}")
                    group_ids = ["grup_0"]

                for group_id in group_ids:
                    # Grup için dosya oluştur (ilk seferde)
                    if group_id not in output_files:
                        group_info = await group_manager.get_group_info(group_id)
                        filename = await generate_output_filename(group_info)

                        output_dir = config.paths.OUTPUT_DIR
                        output_dir.mkdir(parents=True, exist_ok=True)
                        file_path = output_dir / filename

                        def create_writer():
                            wb_out = xlsxwriter.Workbook(
                                file_path, {'constant_memory': True}
                            )
                            ws_out = wb_out.add_worksheet("Veriler")
                            ws_out.write_row(0, 0, self.headers)
                            ws_out.set_column(0, len(self.headers) - 1, 15)
                            return wb_out, ws_out, file_path

                        wb_out, ws_out, file_path = await loop.run_in_executor(
                            self._executor, create_writer
                        )

                        output_files[group_id] = {
                            "writer": wb_out,
                            "worksheet": ws_out,
                            "file_path": file_path,
                            "row_count": 1,  # başlık satırı
                            "cities": set()
                        }

                    # Blok verilerini kopyala
                    row_count = await self._copy_block_to_group(
                        ws, block, output_files[group_id], loop
                    )

                    output_files[group_id]["cities"].add(city)
                    total_rows += row_count

            # Writer'ları kapat
            final_outputs = await self._close_writers(output_files, loop)
            wb.close()

            return {
                "success": True,
                "output_files": final_outputs,
                "total_blocks": len(blocks),
                "total_rows": total_rows,
                "blocks": blocks
            }

        except Exception as e:
            logger.error(f"❌ Blok işleme hatası: {e}", exc_info=True)
            return {
                "success": False,
                "error": str(e)
            }







    async def _copy_block_to_group(self, source_ws, block, group_data, loop):
        """Basit fix: Her satırda İL sütununa şehir adını yaz"""
        row_count = 0
        city_name = block["city"]
        
        for row in range(block["start_row"], block["end_row"] + 1):
            has_data = False
            row_data = []
            
            for col in range(1, block["max_column"] + 1):

                cell_value = source_ws.cell(row=row, column=col).value

                # Excel tarihlerini düzelt
                if isinstance(cell_value, (datetime, date)):
                    cell_value = cell_value.strftime("%d.%m.%Y")

                                
                
                
                
                
                row_data.append(cell_value)
                if cell_value is not None:
                    has_data = True
            
            if has_data:
                # FIX: İL sütununa (2. sütun) şehir adını yaz
                if len(row_data) > 1:  # En az 2 sütun varsa
                    row_data[1] = city_name  # 2. sütun = İL
                
                def write_row():
                    group_data["worksheet"].write_row(
                        group_data["row_count"], 0, row_data
                    )
                
                await loop.run_in_executor(self._executor, write_row)
                group_data["row_count"] += 1
                row_count += 1
        
        return row_count


    async def _close_writers(self, output_files, loop):
        """Writer'ları kapat ve sonuçları döndür"""
        final_outputs = {}
        
        for group_id, data in output_files.items():
            # Writer'ı kapat
            def close_writer():
                data["writer"].close()
            
            await loop.run_in_executor(self._executor, close_writer)
            
            # Boş dosyaları sil
            if data["row_count"] <= 1:  # Sadece başlık varsa
                try:
                    data["file_path"].unlink()
                    logger.info(f"🗑️ Boş dosya silindi: {data['file_path'].name}")
                    continue
                except:
                    pass
            
            final_outputs[group_id] = {
                "filename": data["file_path"].name,
                "path": data["file_path"],
                "row_count": data["row_count"] - 1,  # Başlık hariç
                "cities": list(data["cities"])
            }
            
            logger.info(f"📄 {data['file_path'].name}: {data['row_count']-1} satır")
        
        return final_outputs


async def split_excel_by_blocks(input_path: str, headers: List[str]) -> Dict[str, Any]:
    """Blok bazlı Excel bölme fonksiyonu"""
    splitter = BlockExcelSplitter(input_path, headers)
    return await splitter.process_blocks()