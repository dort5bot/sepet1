"""
Özellikler:
işlem: Merge ve GSM ekleme → sonuc.xlsx
işlem: City/İL düzenleme → sonuc_final.xlsx
İşlemler sıralı olarak tek betikte çalışıyor.

"""
import pandas as pd
from pathlib import Path
from typing import List

# -------------------------------------------------
# işlem-1 → dosya1(ham)  ile dosya2(tel)  arasında eşleştirme yapılır İL-TC-GSM oluşur
# -------------------------------------------------
# -------------------------------------------------
# 1) Excel oku – normalize ve tekrar eden kolonları düzelt
# -------------------------------------------------
def normalize_and_deduplicate_columns(df: pd.DataFrame) -> pd.DataFrame:
    new_cols = []
    seen = {}
    for c in df.columns:
        col = str(c).strip()
        if not col:
            col = "auto"
        if col in seen:
            seen[col] += 1
            col = f"{col}_{seen[col]}"
        else:
            seen[col] = 1
        new_cols.append(col)
    df.columns = new_cols
    return df

def read_excel_smart(path: Path) -> pd.DataFrame:
    df = pd.read_excel(path)
    df.columns = [str(c).strip() if str(c).strip() else "" for c in df.columns]
    return normalize_and_deduplicate_columns(df)

# -------------------------------------------------
# 2) Kolon bulucu (case/space insensitive)
# -------------------------------------------------
def find_col(df: pd.DataFrame, name: str) -> str:
    name = name.strip().upper()
    for c in df.columns:
        if str(c).strip().upper() == name:
            return c
    raise ValueError(f"Zorunlu kolon bulunamadı: {name}")

# -------------------------------------------------
# 3) ANA MERGE (TC ASLA SİLİNMEZ)
# -------------------------------------------------
def build_merged_excel(ham_dosya: Path, tel_dosya: Path, output_path: Path) -> Path:
    df_ham = read_excel_smart(ham_dosya)
    df_tel = read_excel_smart(tel_dosya)

    ham_tc = find_col(df_ham, "TC")
    tel_tc = find_col(df_tel, "TC")
    tel_col = find_col(df_tel, "TEL")

    # TC sütunlarını string yap ve .0 temizle
    df_ham[ham_tc] = df_ham[ham_tc].astype(str).str.replace(r"\.0$", "", regex=True).str.strip()
    df_tel[tel_tc] = df_tel[tel_tc].astype(str).str.replace(r"\.0$", "", regex=True).str.strip()

    df_tel_small = df_tel[[tel_tc, tel_col]]

    merged = df_ham.merge(
        df_tel_small,
        how="left",
        left_on=ham_tc,
        right_on=tel_tc,
        suffixes=("", "_TEL")
    )

    # Sağdan gelen TC'yi sil
    if f"{tel_tc}_TEL" in merged.columns:
        merged.drop(columns=[f"{tel_tc}_TEL"], inplace=True)

    # GSM'yi TC yanına koy
    tc_index = merged.columns.get_loc(ham_tc)
    if "GSM" not in merged.columns:
        merged.insert(tc_index + 1, "GSM", merged[tel_col])
    else:
        merged["GSM"] = merged[tel_col]

    # Geçici TEL kolonunu sil
    merged.drop(columns=[tel_col], inplace=True)

    # Merge sonucu kaydet
    merged.to_excel(output_path, index=False)
    return output_path

# -------------------------------------------------
# işlem-2 → City/İL düzenleme kovaya tam uyumu yapı
# -------------------------------------------------

import re
import unicodedata

class CityProcessor:
    _CITY_DICT = None
    _CITY_REGEX = None

    @staticmethod
    def normalize_turkish(text: str) -> str:
        if not isinstance(text, str):
            return ""

        text = text.replace("İ", "I").replace("ı", "i").lower()
        text = unicodedata.normalize("NFKD", text)
        text = "".join(c for c in text if not unicodedata.combining(c))

        return (
            text.replace("ş", "s")
                .replace("ç", "c")
                .replace("ü", "u")
                .replace("ö", "o")
                .replace("ğ", "g")
        )

    @classmethod
    def get_city_dict(cls):
        if cls._CITY_DICT is None:
            raw = [
                "Adana","Adıyaman","Afyon","Afyonkarahisar","Ağrı","Aksaray","Amasya","Ankara",
                "Antalya","Ardahan","Artvin","Aydın",
                "Balıkesir","Bartın","Batman","Bayburt","Bilecik","Bingöl","Bitlis","Bolu","Burdur","Bursa",
                "Çanakkale","Çankırı","Çorum","Denizli","Diyarbakır","Düzce",
                "Edirne","Elazığ","Erzincan","Erzurum","Eskişehir",
                "Gaziantep","Giresun","Gümüşhane","Hakkari","Hatay",
                "Iğdır","Isparta","İstanbul","İzmir","İçel",
                "Kahramanmaraş","Karabük","Karaman","Kars","Kastamonu","Kayseri","Kilis",
                "Kırıkkale","Kırklareli","Kırşehir","Kocaeli","Konya","Kütahya",
                "Malatya","Manisa","Mardin","Mersin","Muğla","Muş",
                "Nevşehir","Niğde","Ordu","Osmaniye","Rize",
                "Sakarya","Samsun","Siirt","Sinop","Sivas",
                "Şanlıurfa","Şırnak","Tekirdağ","Tokat","Trabzon","Tunceli",
                "Uşak","Van","Yalova","Yozgat","Zonguldak"
            ]
            cls._CITY_DICT = {cls.normalize_turkish(c): c for c in raw}
        return cls._CITY_DICT

    @classmethod
    def get_city_regex(cls):
        if cls._CITY_REGEX is None:
            city_dict = cls.get_city_dict()
            pattern = r"\b(" + "|".join(sorted(city_dict.keys(), key=len, reverse=True)) + r")\b"
            cls._CITY_REGEX = re.compile(pattern)
        return cls._CITY_REGEX


def process_city_il(input_file: Path, output_file: Path):
    df = pd.read_excel(input_file)

    # 1- İL → City
    if "İL" in df.columns:
        df.rename(columns={"İL": "City"}, inplace=True)
    else:
        df["City"] = ""

    # 2- Yeni İL kolonu
    if "İL" not in df.columns:
        df.insert(df.columns.get_loc("City") + 1, "İL", "")

    city_dict = CityProcessor.get_city_dict()
    city_regex = CityProcessor.get_city_regex()

    # 3- TEK SEFERDE normalize (vektörel)
    city_norm = (
        df["City"]
        .astype(str)
        .map(CityProcessor.normalize_turkish)
    )

    # 4- Regex ile şehir yakala
    found = city_norm.str.extract(city_regex, expand=False)

    # 5- Orijinal şehir adına map et
    df["İL"] = found.map(city_dict)


    # 6- ffill (tek satır!)
    df["İL"] = df["İL"].ffill().infer_objects(copy=False)


    # 7- City sil
    df.drop(columns=["City"], inplace=True)

    df.to_excel(output_file, index=False)
    print(f"✅ İşlem tamamlandı → {output_file}")



# -------------------------------------------------
# 5) Ana program
# -------------------------------------------------
if __name__ == "__main__":
    ham = Path("ham.xlsx")
    tel = Path("tel.xlsx")
    merged_file = Path("sonuc.xlsx")
    final_file = Path("sonuc_final.xlsx")

    # 1. işlem: Merge
    build_merged_excel(ham, tel, merged_file)
    print("✅ 1. işlem tamamlandı → sonuc.xlsx oluşturuldu")

    # 2. işlem: City/İL düzenleme
    process_city_il(merged_file, final_file)
