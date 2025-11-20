# Excel Temizleyici (utils/excel_cleaner.py) - Geliştirilmiş Asenkron Versiyon
# Sütun genişliği otomatik olarak içeriğe göre ayarlanır, minimum 10, maksimum 25 birim
# Tam asenkron uyumlu, performans iyileştirmeli
# 16-11-2025 22:20

import asyncio
from openpyxl import load_workbook, Workbook
from openpyxl.utils import get_column_letter
from openpyxl.worksheet.worksheet import Worksheet
from typing import Dict, List, Tuple, Any, Optional
from utils.logger import logger
import tempfile
import os
import aiofiles
from pathlib import Path
import pandas as pd
from concurrent.futures import ThreadPoolExecutor

# Sabitler
MAX_HEADER_SEARCH_ROWS = 5
MIN_COLUMN_WIDTH = 10
MAX_COLUMN_WIDTH = 25
HEADER_ROW_BUFFER = 2
MAX_FILE_SIZE_MB = 50  # Maksimum dosya boyutu
CHUNK_SIZE = 1000  # Büyük dosyalar için chunk boyutu

class AsyncExcelCleaner:
    """Excel dosya temizleme işlemlerini asenkron olarak yöneten sınıf"""
    
    def __init__(self):
        self.required_columns = {"TARİH", "İL"}
        self.thread_pool = ThreadPoolExecutor(max_workers=4)
    
    async def _check_file_size(self, file_path: str) -> bool:
        """Dosya boyutunu kontrol eder"""
        try:
            file_size = os.path.getsize(file_path) / (1024 * 1024)  # MB cinsinden
            if file_size > MAX_FILE_SIZE_MB:
                logger.warning(f"Dosya boyutu çok büyük: {file_size:.2f}MB")
                return False
            return True
        except Exception as e:
            logger.error(f"Dosya boyutu kontrol hatası: {e}")
            return False
    
    async def _find_header_row(self, ws: Worksheet) -> int:
        """Başlık satırını bulur (asenkron wrapper)"""
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(
            self.thread_pool, 
            self._sync_find_header_row, 
            ws
        )
    
    def _sync_find_header_row(self, ws: Worksheet) -> int:
        """Başlık satırını senkron olarak bulur"""
        for row in range(1, MAX_HEADER_SEARCH_ROWS + 1):
            if any(ws.cell(row=row, column=col).value 
                   for col in range(1, ws.max_column + 1)):
                return row
        return 1
    
    async def _clean_headers(self, ws: Worksheet, header_row: int) -> List[str]:
        """Başlıkları temizler ve düzenler (asenkron wrapper)"""
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(
            self.thread_pool,
            self._sync_clean_headers,
            ws, header_row
        )
    
    def _sync_clean_headers(self, ws: Worksheet, header_row: int) -> List[str]:
        """Başlıkları senkron olarak temizler"""
        headers = []
        for col in range(1, ws.max_column + 1):
            cell_value = ws.cell(row=header_row, column=col).value
            clean_value = (str(cell_value).strip().upper() 
                          if cell_value else f"UNKNOWN_{col}")
            headers.append(clean_value)
        return headers
    
    def _find_required_columns(self, headers: List[str]) -> Dict[str, int]:
        """Gerekli sütunların indekslerini bulur"""
        column_indices = {}
        
        for idx, header in enumerate(headers, 1):
            if "TARİH" in header and "TARİH" not in column_indices:
                column_indices["TARİH"] = idx
            elif "İL" in header and "İL" not in column_indices:
                column_indices["İL"] = idx
        
        missing_columns = self.required_columns - set(column_indices.keys())
        if missing_columns:
            raise ValueError(f"Eksik sütunlar: {', '.join(missing_columns)}")
        
        return column_indices
    
    def _organize_headers(self, headers: List[str], column_indices: Dict[str, int]) -> List[str]:
        """Başlıkları yeniden düzenler"""
        new_headers = ["TARİH", "İL"]
        other_headers = []
        
        used_indices = set(column_indices.values())
        for idx, header in enumerate(headers, 1):
            if idx not in used_indices and header not in new_headers:
                other_headers.append(header)
        
        new_headers.extend(other_headers)
        return new_headers
    
    async def _copy_data_chunked(self, source_ws: Worksheet, target_ws: Worksheet, 
                               header_row: int, column_indices: Dict[str, int]) -> int:
        """Verileri chunk'lar halinde asenkron olarak kopyalar"""
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(
            self.thread_pool,
            self._sync_copy_data_chunked,
            source_ws, target_ws, header_row, column_indices
        )
    
    def _sync_copy_data_chunked(self, source_ws: Worksheet, target_ws: Worksheet, 
                              header_row: int, column_indices: Dict[str, int]) -> int:
        """Verileri chunk'lar halinde senkron olarak kopyalar"""
        date_idx = column_indices["TARİH"]
        city_idx = column_indices["İL"]
        other_columns = [col for col in range(1, source_ws.max_column + 1) 
                        if col not in [date_idx, city_idx]]
        
        total_rows = source_ws.max_row - header_row
        new_row_idx = 2
        
        # Büyük dosyalar için chunk işleme
        for chunk_start in range(header_row + 1, source_ws.max_row + 1, CHUNK_SIZE):
            chunk_end = min(chunk_start + CHUNK_SIZE, source_ws.max_row + 1)
            
            for row in range(chunk_start, chunk_end):
                # Temel sütunları kopyala
                target_ws.cell(row=new_row_idx, column=1, 
                              value=source_ws.cell(row=row, column=date_idx).value)
                target_ws.cell(row=new_row_idx, column=2, 
                              value=source_ws.cell(row=row, column=city_idx).value)
                
                # Diğer sütunları kopyala
                for new_col_idx, source_col in enumerate(other_columns, 3):
                    target_ws.cell(row=new_row_idx, column=new_col_idx,
                                  value=source_ws.cell(row=row, column=source_col).value)
                
                new_row_idx += 1
            
            # İlerleme durumunu logla (büyük dosyalar için)
            if total_rows > CHUNK_SIZE:
                progress = min(100, ((chunk_start - header_row) / total_rows) * 100)
                if progress % 25 == 0:  # Her %25'te bir log
                    logger.info(f"Excel işleme devam ediyor: %{progress:.1f}")
        
        return new_row_idx - 2
    
    async def _adjust_column_widths(self, ws: Worksheet):
        """Sütun genişliklerini asenkron olarak ayarlar"""
        loop = asyncio.get_event_loop()
        await loop.run_in_executor(
            self.thread_pool,
            self._sync_adjust_column_widths,
            ws
        )
    
    def _sync_adjust_column_widths(self, ws: Worksheet):
        """Sütun genişliklerini senkron olarak ayarlar"""
        for column_cells in ws.columns:
            if not column_cells:
                continue
                
            max_length = 0
            for cell in column_cells:
                if cell.value:
                    max_length = max(max_length, len(str(cell.value)))
            
            column_letter = get_column_letter(column_cells[0].column)
            adjusted_width = min(MAX_COLUMN_WIDTH, 
                               max(max_length + HEADER_ROW_BUFFER, MIN_COLUMN_WIDTH))
            ws.column_dimensions[column_letter].width = adjusted_width
    
    async def _save_workbook(self, wb: Workbook, file_path: str):
        """Workbook'u asenkron olarak kaydeder"""
        loop = asyncio.get_event_loop()
        await loop.run_in_executor(
            self.thread_pool,
            self._sync_save_workbook,
            wb, file_path
        )
    
    def _sync_save_workbook(self, wb: Workbook, file_path: str):
        """Workbook'u senkron olarak kaydeder"""
        wb.save(file_path)
    
    async def _load_workbook(self, file_path: str) -> Workbook:
        """Workbook'u asenkron olarak yükler"""
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(
            self.thread_pool,
            load_workbook,
            file_path
        )
    
    async def clean_excel_headers(self, input_path: str) -> Dict[str, Any]:
        """
        Excel dosyasının başlıklarını asenkron olarak temizler ve düzenler
        
        Args:
            input_path: Giriş Excel dosyası yolu
            
        Returns:
            İşlem sonucunu içeren sözlük
        """
        wb = new_wb = None
        temp_path = None
        
        try:
            # Dosya varlığını kontrol et
            if not os.path.exists(input_path):
                raise FileNotFoundError(f"Dosya bulunamadı: {input_path}")
            
            # Dosya boyutunu kontrol et
            if not await self._check_file_size(input_path):
                return {
                    "success": False, 
                    "error": f"Dosya boyutu {MAX_FILE_SIZE_MB}MB'den büyük"
                }
            
            logger.info(f"Excel temizleme başlatıldı: {input_path}")
            
            # Kaynak dosyayı asenkron yükle
            wb = await self._load_workbook(input_path)
            ws = wb.active
            
            # Başlık satırını asenkron bul
            header_row = await self._find_header_row(ws)
            logger.info(f"Başlık satırı bulundu: {header_row}")
            
            # Başlıkları asenkron temizle
            headers = await self._clean_headers(ws, header_row)
            logger.info(f"Temizlenen başlıklar: {headers}")
            
            # Gerekli sütunları bul
            column_indices = self._find_required_columns(headers)
            logger.info(f"Gerekli sütun indeksleri: {column_indices}")
            
            # Başlıkları düzenle
            new_headers = self._organize_headers(headers, column_indices)
            logger.info(f"Yeni başlık düzeni: {new_headers}")
            
            # Yeni workbook oluştur
            new_wb = Workbook()
            new_ws = new_wb.active
            new_ws.title = "Düzenlenmiş Veri"
            
            # Yeni başlıkları yaz
            for col_idx, header in enumerate(new_headers, 1):
                new_ws.cell(row=1, column=col_idx, value=header)
            
            # Verileri asenkron kopyala
            row_count = await self._copy_data_chunked(ws, new_ws, header_row, column_indices)
            logger.info(f"Toplam {row_count} satır kopyalandı")
            
            # Sütun genişliklerini asenkron ayarla
            await self._adjust_column_widths(new_ws)
            
            # Geçici dosyaya asenkron kaydet
            temp_file = tempfile.NamedTemporaryFile(delete=False, suffix=".xlsx")
            temp_path = temp_file.name
            temp_file.close()
            
            await self._save_workbook(new_wb, temp_path)
            logger.info(f"Geçici dosya oluşturuldu: {temp_path}")
            
            return {
                "success": True,
                "temp_path": temp_path,
                "headers": new_headers,
                "row_count": row_count,
                "original_headers": headers,
                "processed_at": pd.Timestamp.now().isoformat()
            }
            
        except Exception as e:
            logger.error(f"Excel temizleme hatası: {e}")
            # Geçici dosyayı temizle
            if temp_path and os.path.exists(temp_path):
                try:
                    os.unlink(temp_path)
                except Exception as cleanup_error:
                    logger.error(f"Geçici dosya temizleme hatası: {cleanup_error}")
            
            return {
                "success": False, 
                "error": str(e),
                "error_type": type(e).__name__
            }
        
        finally:
            # Kaynakları temizle
            if wb:
                wb.close()
            if new_wb:
                new_wb.close()
    
    async def batch_clean_excel_files(self, file_paths: List[str]) -> Dict[str, Any]:
        """
        Birden fazla Excel dosyasını asenkron olarak temizler
        
        Args:
            file_paths: Excel dosya yolları listesi
            
        Returns:
            Toplu işlem sonuçları
        """
        tasks = [self.clean_excel_headers(file_path) for file_path in file_paths]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        successful = []
        failed = []
        
        for i, result in enumerate(results):
            if isinstance(result, Exception):
                failed.append({
                    "file": file_paths[i],
                    "error": str(result)
                })
            elif result.get("success"):
                successful.append({
                    "file": file_paths[i],
                    "temp_path": result["temp_path"],
                    "row_count": result["row_count"]
                })
            else:
                failed.append({
                    "file": file_paths[i],
                    "error": result.get("error", "Bilinmeyen hata")
                })
        
        return {
            "total_files": len(file_paths),
            "successful": successful,
            "failed": failed,
            "success_rate": len(successful) / len(file_paths) * 100 if file_paths else 0
        }
    
    def __del__(self):
        """Nesne yok edilirken thread pool'u temizle"""
        if hasattr(self, 'thread_pool'):
            self.thread_pool.shutdown(wait=False)
