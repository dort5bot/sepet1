# utils/group_manager.py
"""
Basitleştirilmiş JSON + Tüm Async Özellikler
"""
import asyncio
import json
import unicodedata
import re
from pathlib import Path
from typing import Dict, List, Optional, Any
from dataclasses import dataclass, asdict
import aiofiles
from config import config
from utils.logger import logger

@dataclass
class GroupConfig:
    """Grup yapılandırma veri modeli"""
    group_id: str
    group_name: str
    email_recipients: List[str]
    cities: List[str]

class GroupManager:
    """TAM ASYNC Group Manager - Basitleştirilmiş"""
    
    def __init__(self):
        self.groups: Dict[str, GroupConfig] = {}
        self.groups_file = config.paths.GROUPS_DIR / "groups.json"
        self._lock = asyncio.Lock()
        self._loaded = False
        
        self.city_to_group: Dict[str, List[str]] = {}
        self.group_cache: Dict[str, Dict[str, Any]] = {}
        self._initialized = False
        self._init_lock = asyncio.Lock()
    
    async def _ensure_initialized(self):
        """Async başlatma garantisi"""
        if self._initialized:
            return
            
        async with self._init_lock:
            if not self._initialized:
                await self.initialize()
    
    def normalize_city_name(self, city_name: str) -> str:
        """Şehir normalizasyonu"""
        if not city_name or not isinstance(city_name, str):
            return ""
        
        # Unicode normalizasyonu
        city_name = unicodedata.normalize('NFKD', city_name)
        
        # Türkçe karakter dönüşümü
        turkish_to_english = str.maketrans(
            'ğĞıİöÖüÜşŞçÇ',
            'gGiIoOuUsScC'
        )
        normalized = city_name.translate(turkish_to_english)
        
        # Temizleme ve formatlama
        normalized = re.sub(r'[^A-Z0-9\s]', '', normalized.upper().strip())
        normalized = re.sub(r'\s+', ' ', normalized)
        
        return normalized
    
    async def _build_city_mapping(self) -> Dict[str, List[str]]:
        """Şehir-grup mapping"""
        mapping = {}
        total_cities = 0
        
        for group_id, group_config in self.groups.items():
            for city in group_config.cities:
                normalized_city = self.normalize_city_name(city)
                if normalized_city:
                    mapping.setdefault(normalized_city, []).append(group_id)
                    total_cities += 1
        
        # Varsayılan değerler
        mapping.update({
            "": ["grup_0"],
            "UNKNOWN": ["grup_0"]
        })
        
        logger.debug(f"Şehir mapping oluşturuldu: {total_cities} şehir, {len(mapping)} normalized entry")
        return mapping

    async def initialize(self) -> None:
        """Async başlatma"""
        async with self._init_lock:
            await self.load_groups()
            self.city_to_group = await self._build_city_mapping()
            self._initialized = True
            logger.info(f"✅ GroupManager başlatıldı: {len(self.groups)} grup, {len(self.city_to_group)} şehir mapping")

    async def load_groups(self) -> None:
        """Grupları async olarak yükle"""
        async with self._lock:
            try:
                if not await aiofiles.os.path.exists(self.groups_file):
                    logger.warning(f"Groups file not found: {self.groups_file}")
                    await self._create_default_groups()
                    return

                async with aiofiles.open(self.groups_file, 'r', encoding='utf-8') as f:
                    content = await f.read()
                    groups_data = json.loads(content) if content else {}

                self.groups.clear()
                self.group_cache.clear()
                
                # MEVCUT JSON FORMATINI DESTEKLE
                if "groups" in groups_data:
                    for group_data in groups_data["groups"]:
                        group_id = group_data.get("group_id")
                        if group_id:
                            self.groups[group_id] = GroupConfig(
                                group_id=group_id,
                                group_name=group_data.get("group_name", group_id),
                                email_recipients=group_data.get("email_recipients", []),
                                cities=group_data.get("cities", [])
                            )
                else:
                    for group_id, group_data in groups_data.items():
                        self.groups[group_id] = GroupConfig(
                            group_id=group_id,
                            group_name=group_data.get('group_name', group_id),
                            email_recipients=group_data.get('email_recipients', []),
                            cities=group_data.get('cities', [])
                        )

                logger.info(f"✅ {len(self.groups)} grup async yüklendi")
                self._loaded = True

            except Exception as e:
                logger.error(f"❌ Grup yükleme hatası: {e}")
                await self._create_default_groups()

    async def _create_default_groups(self) -> None:
        """Varsayılan grupları oluştur"""
        default_groups = {
            "grup_0": GroupConfig(
                group_id="grup_0",
                group_name="Eşleşmeyen Veriler",
                email_recipients=["ydf.kum@gmail.com"],
                cities=[]
            ),
            "grup_1": GroupConfig(
                group_id="grup_1",
                group_name="antalya_sube",
                email_recipients=["ydf.kum@gmail.com", "dersdep@gmail.com"],
                cities=[
                    "antalya_sube", "Afyon", "Aksaray", "Ankara", "Antalya", 
                    "Burdur", "Çankırı", "Isparta", "Karaman", "Kayseri", 
                    "Kırıkkale", "Kırşehir", "Konya", "Uşak"
                ]
            )
        }
        
        self.groups.update(default_groups)
        await self.save_groups()
        logger.info("✅ Varsayılan gruplar oluşturuldu")

    async def save_groups(self) -> bool:
        """Grupları async olarak kaydet"""
        async with self._lock:
            try:
                self.groups_file.parent.mkdir(parents=True, exist_ok=True)
                
                save_data = {
                    "groups": [
                        {
                            "group_id": group_id,
                            "group_name": group_config.group_name,
                            "email_recipients": group_config.email_recipients,
                            "cities": group_config.cities
                        }
                        for group_id, group_config in self.groups.items()
                    ]
                }

                async with aiofiles.open(self.groups_file, 'w', encoding='utf-8') as f:
                    await f.write(json.dumps(save_data, indent=2, ensure_ascii=False))

                logger.info(f"✅ {len(self.groups)} grup kaydedildi")
                return True

            except Exception as e:
                logger.error(f"❌ Grup kaydetme hatası: {e}")
                return False

    async def get_groups_for_city(self, city_name: str) -> List[str]:
        """Şehir için grupları bul - async"""
        await self._ensure_initialized()
        
        if not city_name:
            return ["grup_0"]
        
        normalized_city = self.normalize_city_name(city_name)
        return self.city_to_group.get(normalized_city, ["grup_0"])

    async def get_group_info(self, group_id: str) -> Dict[str, Any]:
        """Grup bilgisini al - async"""
        await self._ensure_initialized()
        
        # Cache kontrolü
        if group_id in self.group_cache:
            return self.group_cache[group_id]
        
        # Grup arama
        group_config = self.groups.get(group_id)
        if not group_config:
            # Varsayılan grup
            group_config = GroupConfig(
                group_id=group_id,
                group_name="Bilinmeyen Grup",
                email_recipients=[],
                cities=[]
            )
        
        result = asdict(group_config)
        self.group_cache[group_id] = result
        return result

    async def get_group_for_city(self, city_name: str) -> Optional[Dict[str, Any]]:
        """Şehir için grup bul - async"""
        await self._ensure_initialized()

        group_ids = await self.get_groups_for_city(city_name)
        if group_ids and group_ids[0] != "grup_0":
            return await self.get_group_info(group_ids[0])
        
        return None

    async def refresh_groups(self):
        """Grupları yenile - async"""
        async with self._init_lock:
            await self.load_groups()
            self.city_to_group = await self._build_city_mapping()
            self.group_cache.clear()
            logger.info("✅ Gruplar async olarak yenilendi")

    async def get_all_groups(self) -> List[Dict[str, Any]]:
        """Tüm grupları async al"""
        await self._ensure_initialized()
        return [asdict(group) for group in self.groups.values()]

    async def update_group(self, group_id: str, **kwargs) -> bool:
        """Grubu async güncelle"""
        await self._ensure_initialized()
        
        async with self._lock:
            if group_id not in self.groups:
                return False

            try:
                group_config = self.groups[group_id]
                valid_fields = ["group_name", "email_recipients", "cities"]
                
                for key, value in kwargs.items():
                    if key in valid_fields and hasattr(group_config, key):
                        setattr(group_config, key, value)

                # Cache'i temizle ve mapping'i yenile
                self.group_cache.pop(group_id, None)
                self.city_to_group = await self._build_city_mapping()
                
                await self.save_groups()
                logger.info(f"✅ Grup güncellendi: {group_id}")
                return True

            except Exception as e:
                logger.error(f"❌ Grup güncelleme hatası: {e}")
                return False

    async def create_group(self, group_id: str, group_name: str, 
                          email_recipients: List[str], cities: List[str]) -> bool:
        """Yeni grup async oluştur"""
        await self._ensure_initialized()
        
        async with self._lock:
            if group_id in self.groups:
                return False

            try:
                self.groups[group_id] = GroupConfig(
                    group_id=group_id,
                    group_name=group_name,
                    email_recipients=email_recipients,
                    cities=cities
                )

                # Mapping'i yenile
                self.city_to_group = await self._build_city_mapping()
                await self.save_groups()
                
                logger.info(f"✅ Yeni grup oluşturuldu: {group_id}")
                return True

            except Exception as e:
                logger.error(f"❌ Grup oluşturma hatası: {e}")
                return False

    async def get_cities_statistics(self) -> Dict[str, Any]:
        """Şehir istatistiklerini async al"""
        await self._ensure_initialized()
        
        total_cities = sum(len(group.cities) for group in self.groups.values())
        unique_cities = set()
        
        for group in self.groups.values():
            for city in group.cities:
                unique_cities.add(self.normalize_city_name(city))
        
        return {
            "total_groups": len(self.groups),
            "total_cities": total_cities,
            "unique_cities": len(unique_cities),
            "mapping_entries": len(self.city_to_group)
        }

# Global async instance
group_manager = GroupManager()

# Async initializer
async def initialize_group_manager():
    """Group manager'ı async başlat"""
    await group_manager.initialize()