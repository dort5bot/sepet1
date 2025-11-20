# utils/excel_splitter.py
"""
Excel dosyasını gruplara ayıran ana fonksiyon
TAM ASYNC & TAM UYUMLU VERSİYON
"""

from openpyxl import load_workbook, Workbook
from openpyxl.utils import get_column_letter
from typing import Dict, List, Tuple, Any, Set, Optional
import asyncio
import aiofiles

from utils.group_manager import group_manager
from utils.file_namer import generate_output_filename
from utils.logger import logger
from config import config


class ExcelSplitter:
    def __init__(self):
        self.workbooks: Dict[str, Workbook] = {}
        self.sheets: Dict[str, Any] = {}  
        self.row_counts: Dict[str, int] = {}
        self.headers: List[str] = []
        self.city_mapping_stats: Dict[str, int] = {}
        self._processing_lock = asyncio.Lock()
    
    async def _initialize_workbook(self, group_id: str) -> None:
        """Workbook başlatma - TAM ASYNC"""
        async with self._processing_lock:
            if group_id in self.workbooks:
                return
                
            # Workbook oluşturma işlemini async yap
            loop = asyncio.get_event_loop()
            wb = await loop.run_in_executor(None, Workbook)
            ws = wb.active
            ws.title = "Veriler"
            
            # Başlık satırını async olarak yaz
            for col_idx, header in enumerate(self.headers, 1):
                ws.cell(row=1, column=col_idx, value=header)
            
            await self._adjust_column_widths_async(ws)
            
            self.workbooks[group_id] = wb
            self.sheets[group_id] = ws
            self.row_counts[group_id] = 1
            self.city_mapping_stats[group_id] = 0
    
    async def _adjust_column_widths_async(self, worksheet, width: int = 25) -> None:
        """Sütun genişliklerini ASYNC ayarlar"""
        def sync_adjust_columns():
            for column_cells in worksheet.columns:
                length = max(len(str(cell.value)) if cell.value else 0 for cell in column_cells)
                column_letter = get_column_letter(column_cells[0].column)
                worksheet.column_dimensions[column_letter].width = min(
                    width, max(length + 2, 10)
                )
        
        loop = asyncio.get_event_loop()
        await loop.run_in_executor(None, sync_adjust_columns)
    
    async def _process_row(self, row: Tuple, row_idx: int) -> Dict[str, Any]:
        """Tek bir satırı TAM ASYNC olarak işler"""
        try:
            if not any(cell for cell in row if cell is not None):  # Geliştirilmiş boş satır kontrolü
                return {"groups": set(), "city": None, "row_data": row}
            
            city = row[1] if len(row) > 1 and row[1] else None
            
            # Async grup yöneticisini çağır
            group_ids = await group_manager.get_groups_for_city(city)
            
            return {
                "groups": set(group_ids),
                "city": city,
                "row_data": row
            }
        except Exception as e:
            logger.error(f"Satır işleme hatası (satır {row_idx}): {e}")
            return {"groups": set(), "city": None, "row_data": row}
    
    async def _add_row_to_group(self, group_id: str, row_data: Tuple) -> None:
        """Satırı ilgili gruba ASYNC ekler"""
        await self._initialize_workbook(group_id)
        
        async with self._processing_lock:
            ws_dest = self.sheets[group_id]
            current_row = self.row_counts[group_id] + 1
            
            # Satır yazma işlemini async yap
            def sync_write_row():
                for col_idx, value in enumerate(row_data, 1):
                    ws_dest.cell(row=current_row, column=col_idx, value=value)
                return current_row
            
            loop = asyncio.get_event_loop()
            updated_row = await loop.run_in_executor(None, sync_write_row)
            
            self.row_counts[group_id] = updated_row
            self.city_mapping_stats[group_id] = self.city_mapping_stats.get(group_id, 0) + 1
    
    async def process_excel_file(self, input_path: str, headers: List[str]) -> Dict[str, Any]:
        """Excel dosyasını TAM ASYNC olarak işler - GELİŞTİRİLMİŞ"""
        wb = None
        try:
            # Group manager'ın başlatıldığından emin ol
            await group_manager._ensure_initialized()
            
            self.headers = headers
            self.city_mapping_stats = {}
            
            # Excel dosyasını async olarak yükle
            loop = asyncio.get_event_loop()
            wb = await loop.run_in_executor(
                None, 
                lambda: load_workbook(input_path, read_only=True)
            )
            ws = wb.active
            
            total_rows = ws.max_row - 1
            logger.info(f"İşlenecek toplam satır: {total_rows}")
            
            processed_rows = 0
            unmatched_cities: Set[str] = set()
            batch_tasks = []
            
            # Toplu işleme için batch boyutu
            BATCH_SIZE = 500
            
            # Satırları async olarak işle - batch processing ile
            for row_idx, row in enumerate(ws.iter_rows(min_row=2, values_only=True), start=2):
                # Satır işleme task'ini oluştur
                task = asyncio.create_task(self._process_row(row, row_idx))
                batch_tasks.append(task)
                
                # Batch dolduğunda veya son satıra ulaşıldığında işle
                if len(batch_tasks) >= BATCH_SIZE or row_idx == ws.max_row:
                    # Tüm batch task'lerini bekleyerek paralel işle
                    batch_results = await asyncio.gather(*batch_tasks, return_exceptions=True)
                    
                    for result in batch_results:
                        if isinstance(result, Exception):
                            logger.error(f"Batch işleme hatası: {result}")
                            continue
                            
                        if not result or not result["groups"]:
                            continue
                        
                        # Eşleşmeyen şehirleri kaydet
                        if ("Grup_0" in result["groups"] or "grup_0" in result["groups"]) and len(result["groups"]) == 1:
                            if result["city"]:
                                unmatched_cities.add(str(result["city"]))
                        
                        # Satırı ilgili gruplara ekle
                        group_tasks = [
                            self._add_row_to_group(group_id, result["row_data"])
                            for group_id in result["groups"]
                        ]
                        if group_tasks:
                            await asyncio.gather(*group_tasks, return_exceptions=True)
                        
                        processed_rows += 1
                    
                    # Batch'i temizle
                    batch_tasks.clear()
                    
                    # Optimize edilmiş ilerleme logu
                    if processed_rows % 1000 == 0:
                        logger.info(f"{processed_rows}/{total_rows} satır işlendi")
            
            logger.info(f"İşlem tamamlandı: {processed_rows} satır")
            
            # Eşleşmeyen şehirleri logla
            if unmatched_cities:
                logger.warning(f"Eşleşmeyen şehirler ({len(unmatched_cities)} adet): {list(unmatched_cities)[:10]}{'...' if len(unmatched_cities) > 10 else ''}")
            
            return await self._save_output_files(processed_rows, unmatched_cities)
            
        except Exception as e:
            logger.error(f"Excel ayırma hatası: {e}", exc_info=True)
            return {
                "success": False, 
                "error": str(e),
                "output_files": {},
                "total_rows": 0,
                "matched_rows": 0,
                "unmatched_cities": [],
                "stats": {}
            }
        finally:
            await self._cleanup_resources_async(wb)
    
    async def _save_output_files(self, processed_rows: int, unmatched_cities: Set[str]) -> Dict[str, Any]:
        """Çıktı dosyalarını TAM ASYNC olarak kaydeder"""
        output_files = {}
        
        try:
            # Workbook'ları async olarak kaydet
            save_tasks = []
            for group_id, wb in self.workbooks.items():
                if self.row_counts.get(group_id, 1) > 1:  # Header + en az 1 veri satırı
                    save_tasks.append(self._save_single_workbook(group_id, wb))
            
            if save_tasks:
                saved_files = await asyncio.gather(*save_tasks, return_exceptions=True)
                
                for result in saved_files:
                    if isinstance(result, Exception):
                        logger.error(f"Kaydetme hatası: {result}")
                        continue
                    if result and result[1] is not None:
                        output_files[result[0]] = result[1]
            
            # İstatistikleri hesapla
            matched_rows = sum(
                count - 1 for count in self.row_counts.values() 
                if count and count > 1
            )
            
            return {
                "success": True,
                "output_files": output_files,
                "total_rows": processed_rows,
                "matched_rows": matched_rows,
                "unmatched_cities": list(unmatched_cities),
                "stats": self.city_mapping_stats.copy()
            }
            
        except Exception as e:
            logger.error(f"Çıktı dosyaları kaydetme hatası: {e}")
            return {
                "success": False,
                "error": str(e),
                "output_files": {},
                "total_rows": processed_rows,
                "matched_rows": 0,
                "unmatched_cities": list(unmatched_cities),
                "stats": self.city_mapping_stats.copy()
            }
    
    async def _save_single_workbook(self, group_id: str, wb: Workbook) -> tuple:
        """Tek bir workbook'u TAM ASYNC olarak kaydeder"""
        try:
            # Sütun genişliklerini async güncelle
            if group_id in self.sheets:
                await self._adjust_column_widths_async(self.sheets[group_id])
            
            # Grup bilgilerini async al
            group_info = await group_manager.get_group_info(group_id)
            filename = await generate_output_filename(group_info)
            filepath = config.paths.OUTPUT_DIR / filename
            
            # Klasörü async oluştur
            def sync_create_dir():
                filepath.parent.mkdir(parents=True, exist_ok=True)
            
            loop = asyncio.get_event_loop()
            await loop.run_in_executor(None, sync_create_dir)
            
            # Dosya kaydetme işlemini async yap
            await loop.run_in_executor(None, wb.save, filepath)
            
            logger.info(f"✅ {group_id} için dosya kaydedildi: {filename}")
            
            return (group_id, {
                "path": filepath,
                "row_count": self.row_counts.get(group_id, 1) - 1,
                "filename": filename,
                "matched_cities": self.city_mapping_stats.get(group_id, 0)
            })
            
        except Exception as e:
            logger.error(f"Workbook kaydetme hatası {group_id}: {e}")
            return (group_id, None)
    
    async def _cleanup_resources_async(self, wb=None) -> None:
        """Tüm kaynakları ASYNC temizler"""
        try:
            # Ana workbook'u async kapat
            if wb is not None:
                def sync_close_workbook():
                    try:
                        wb.close()
                    except Exception:
                        pass
                
                loop = asyncio.get_event_loop()
                await loop.run_in_executor(None, sync_close_workbook)
            
            # Oluşturulan workbook'ları async kapat
            close_tasks = []
            for wb_instance in self.workbooks.values():
                def sync_close(wb=wb_instance):
                    try:
                        wb.close()
                    except Exception:
                        pass
                
                close_tasks.append(
                    asyncio.get_event_loop().run_in_executor(None, sync_close)
                )
            
            if close_tasks:
                await asyncio.gather(*close_tasks, return_exceptions=True)
            
        except Exception as e:
            logger.error(f"Resource cleanup hatası: {e}")
        finally:
            # Belleği temizle
            self.workbooks.clear()
            self.sheets.clear() 
            self.row_counts.clear()
            self.city_mapping_stats.clear()


# ASYNC arayüz fonksiyonu - GELİŞTİRİLMİŞ
async def split_excel_by_groups(input_path: str, headers: List[str]) -> Dict[str, Any]:
    """Excel dosyasını TAM ASYNC olarak gruplara ayırır"""
    splitter = ExcelSplitter()
    try:
        logger.info(f"Excel splitter başlatılıyor: {input_path}")
        result = await splitter.process_excel_file(input_path, headers)
        logger.info(f"Excel splitter tamamlandı: {result.get('success', False)}")
        return result
    except Exception as e:
        logger.error(f"split_excel_by_groups hatası: {e}", exc_info=True)
        return {
            "success": False, 
            "error": str(e),
            "output_files": {},
            "total_rows": 0,
            "matched_rows": 0,
            "unmatched_cities": [],
            "stats": {}
        }
    finally:
        await splitter._cleanup_resources_async()


# SYNC arayüz (backward compatibility) - GELİŞTİRİLMİŞ
def split_excel_by_groups_sync(input_path: str, headers: List[str]) -> Dict[str, Any]:
    """Excel dosyasını SYNC olarak gruplara ayırır - ASYNC wrapper"""
    try:
        # Mevcut event loop kontrolü
        try:
            loop = asyncio.get_event_loop()
        except RuntimeError:
            loop = None
        
        if loop and loop.is_running():
            # Async context içinde sync wrapper
            async def run_async():
                return await split_excel_by_groups(input_path, headers)
            
            # Sync olarak async task'i çalıştır
            future = asyncio.run_coroutine_threadsafe(run_async(), loop)
            return future.result(timeout=300)  # 5 dakika timeout
        else:
            # Yeni event loop
            return asyncio.run(split_excel_by_groups(input_path, headers))
            
    except Exception as e:
        logger.error(f"split_excel_by_groups_sync hatası: {e}", exc_info=True)
        return {
            "success": False, 
            "error": str(e),
            "output_files": {},
            "total_rows": 0,
            "matched_rows": 0,
            "unmatched_cities": [],
            "stats": {}
        }