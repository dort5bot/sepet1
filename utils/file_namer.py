#Dosya İsimlendirici (utils/file_namer.py)
# 16-11-2025
"""
Dosya İsimlendirici (utils/file_namer.py)
Tamamen Async Basit Versiyon
20/11
# Yaptığı işler:
- Grup bilgilerine göre dosya adı oluşturur
- Zaman damgası ekler
- Güvenli dosya adları oluşturur (özel karakterleri temizler)
- Dosya uzantılarını yönetir
"""


import asyncio
from datetime import datetime
from typing import Dict
import re


async def generate_output_filename(group_info: Dict, file_extension: str = "xlsx") -> str:
    """Async çıktı dosyası için isim oluşturur
    
    Args:
        group_info: Grup bilgilerini içeren sözlük
        file_extension: Dosya uzantısı (varsayılan: xlsx)
    
    Returns:
        Oluşturulan dosya adı
    """
    group_id = group_info.get("group_id", "Grup_0")
    group_name = group_info.get("group_name", "")
    
    # Async zaman damgası
    timestamp = await asyncio.to_thread(
        lambda: datetime.now().strftime("%m%d_%H%M")
    )
    
    # Grup adı geçerli ve ID'den farklıysa kullan
    if group_name and group_name.strip() and group_name != group_id:
        base_name = f"{group_name}-{timestamp}"
    else:
        base_name = f"{group_id}-{timestamp}"
    
    # Güvenli dosya adı oluştur (async)
    safe_filename = await asyncio.to_thread(
        lambda: "".join(c for c in base_name if c.isalnum() or c in ('-', '_', '.')).rstrip()
    )
    
    clean_extension = file_extension.lstrip('.').lower()
    
    return f"{safe_filename}.{clean_extension}"