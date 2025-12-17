"""
Ana dosya: Başlık her zaman var → sorun yok.
Veri dosyası: Başlık varsa alınır, yoksa v1, v2... atanır → sağlanıyor.
Merge sırasında TC sütunu korunursa, her şey düzgün çalışır

Bu kodun sağladıkları:
Ana dosya TC sütunu korunuyor.
Veri dosyasındaki başlıklar varsa kullanılıyor.
Eksik veya boş başlıklar otomatik olarak v1, v2, v3 gibi atanıyor.
Merge sonrası, df2’den gelen TC sütunu ve geçici kolonlar siliniyor.
TC sütunundan hemen sonra df2 verileri ekleniyor.
"""
# import pandas as pd
# from pathlib import Path

"""def read_excel_with_fallback(path: Path) -> pd.DataFrame:
    df = pd.read_excel(path, header=None)
    first_row = df.iloc[0]

    if first_row.isna().all():
        # Başlık tamamen boşsa otomatik üret
        df.columns = [f"bs{i+1}" for i in range(df.shape[1])]
        df = df.iloc[1:].reset_index(drop=True)
    else:
        # Başlık varsa 1. satır başlık olarak al
        df.columns = df.iloc[0]
        df = df.iloc[1:].reset_index(drop=True)

    return df
"""
"""def build_merged_excel(
    ana_dosya: Path,
    veri_dosya: Path,
    output_path: Path
) -> Path:

    # --- 1. Dosyaları oku ve başlıkları kontrol et ---
    df1 = read_excel_with_fallback(ana_dosya)
    df2 = read_excel_with_fallback(veri_dosya)

    # --- 2. TC sütunlarını bul (case/space insensitive) ---
    tc1 = next(c for c in df1.columns if str(c).strip().upper() == "TC")
    tc2 = next(c for c in df2.columns if str(c).strip().upper() == "TC")

    # --- 3. df2'de TC sonrası 3 sütunu al (veya varsa kadar) ---
    tc2_index = df2.columns.get_loc(tc2)
    veri_cols = df2.columns[tc2_index + 1 : tc2_index + 4].tolist()  # 3 kolon

    # Boş veya NaN başlıkları otomatik doldur
    veri_cols = [
        f"v{i+1}" if (pd.isna(c) or str(c).strip() == "") else c
        for i, c in enumerate(veri_cols)
    ]

    # Eksik kolon varsa üret
    for col in veri_cols:
        if col not in df2.columns:
            df2[col] = pd.NA

    # Sadece gerekli kolonlar
    df2_small = df2[[tc2] + veri_cols]

    # --- 4. Merge ---
    merged = df1.merge(
        df2_small,
        how="left",
        left_on=tc1,
        right_on=tc2
    )

    # --- 5. Sabit hedef: TC'den sonra ilk uygun kolondan başlayacak ---
    tc_index = merged.columns.get_loc(tc1)
    target_index = tc_index + 1  # TC’den sonra hemen yaz

    # --- 6. YOKSA ÜRET (kolon sayısını garanti et) ---
    needed = target_index + len(veri_cols) - len(merged.columns)
    if needed > 0:
        for i in range(needed):
            merged[f"_auto_{i}"] = pd.NA

    # --- 7. Üstüne yaz ---
    print(f"[LOG] target_index: {target_index}, veri_cols: {veri_cols}")
    print(f"[LOG] merged.shape: {merged.shape}")
    merged.iloc[:, target_index : target_index + len(veri_cols)] = merged[veri_cols].values
    print("[LOG] Üstüne yazma tamamlandı")

    # --- 8. Geçici merge kolonlarını sil ---
    merged.drop(columns=[tc2] + veri_cols, inplace=True)

    # --- 9. Kaydet ---
    merged.to_excel(output_path, index=False)

    return output_path
"""



"""def read_excel_with_fallback(path: Path) -> pd.DataFrame:
    df = pd.read_excel(path, header=None)
    first_row = df.iloc[0]

    if first_row.isna().all():
        # Başlık tamamen boşsa otomatik üret
        df.columns = [f"bs{i+1}" for i in range(df.shape[1])]
        df = df.iloc[1:].reset_index(drop=True)
    else:
        # Başlık varsa 1. satır başlık olarak al
        df.columns = df.iloc[0]
        df = df.iloc[1:].reset_index(drop=True)

    return df
"""

"""def build_merged_excel(
    ana_dosya: Path,
    veri_dosya: Path,
    output_path: Path
) -> Path:

    # --- 1. Dosyaları oku ---
    df1 = read_excel_with_fallback(ana_dosya)
    df2 = read_excel_with_fallback(veri_dosya)

    # --- 2. TC sütunlarını bul ---
    tc1 = next(c for c in df1.columns if str(c).strip().upper() == "TC")
    tc2 = next(c for c in df2.columns if str(c).strip().upper() == "TC")

    # --- 3. df2'de TC sonrası 3 sütunu al (varsa kadar) ---
    tc2_index = df2.columns.get_loc(tc2)
    veri_cols = df2.columns[tc2_index + 1 : tc2_index + 4].tolist()

    # Boş başlık varsa otomatik isim ata
    veri_cols = [
        f"v{i+1}" if (pd.isna(c) or str(c).strip() == "") else c
        for i, c in enumerate(veri_cols)
    ]

    # Eksik kolon varsa df2’de üret
    for col in veri_cols:
        if col not in df2.columns:
            df2[col] = pd.NA

    # Sadece gerekli kolonlar
    df2_small = df2[[tc2] + veri_cols]

    # --- 4. Merge ---
    merged = df1.merge(
        df2_small,
        how="left",
        left_on=tc1,
        right_on=tc2,
        suffixes=("", "_y")  # df1 TC kalacak, df2 TC geçici olacak
    )

    # --- 5. TC sonrası yazma ---
    tc_index = merged.columns.get_loc(tc1)
    target_index = tc_index + 1

    # Yeterli sütun yoksa oluştur
    needed = target_index + len(veri_cols) - len(merged.columns)
    if needed > 0:
        for i in range(needed):
            merged[f"_auto_{i}"] = pd.NA

    # Veri sütunlarını kopyala
    merged.iloc[:, target_index : target_index + len(veri_cols)] = merged[veri_cols].values

    # --- 6. Geçici df2 sütunlarını sil ---
    merged.drop(columns=[tc2] + veri_cols, inplace=True)

    # --- 7. Kaydet ---
    merged.to_excel(output_path, index=False)

    return output_path
"""
# l
import pandas as pd
from pathlib import Path
from typing import List


# -------------------------------------------------
# 1) Excel oku – 1.satır HER ZAMAN başlık
# -------------------------------------------------
def read_excel_smart(path: Path) -> pd.DataFrame:
    df = pd.read_excel(path)
    df.columns = [
        str(c).strip() if str(c).strip() else f"auto_{i+1}"
        for i, c in enumerate(df.columns)
    ]
    return df


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
def build_merged_excel(
    ham_dosya: Path,
    tel_dosya: Path,
    output_path: Path
) -> Path:

    # --- oku ---
    df_ham = read_excel_smart(ham_dosya)
    df_tel = read_excel_smart(tel_dosya)

    # --- kolonlar ---
    ham_tc = find_col(df_ham, "TC")
    tel_tc = find_col(df_tel, "TC")
    tel_col = find_col(df_tel, "TEL")

    # --- TC'leri STRING yap + .0 TEMİZLE (ÖNCE!) ---
    df_ham[ham_tc] = (
        df_ham[ham_tc]
        .astype(str)
        .str.replace(r"\.0$", "", regex=True)
        .str.strip()
    )

    df_tel[tel_tc] = (
        df_tel[tel_tc]
        .astype(str)
        .str.replace(r"\.0$", "", regex=True)
        .str.strip()
    )

    # --- SADECE GEREKLİ KOLONLAR (SONRA!) ---
    df_tel_small = df_tel[[tel_tc, tel_col]]

    # --- MERGE ---
    merged = df_ham.merge(
        df_tel_small,
        how="left",
        left_on=ham_tc,
        right_on=tel_tc,
        suffixes=("", "_TEL")
    )

    # --- sağdan gelen TC'yi sil ---
    if f"{tel_tc}_TEL" in merged.columns:
        merged.drop(columns=[f"{tel_tc}_TEL"], inplace=True)

    # --- GSM'yi TC yanına koy ---
    tc_index = merged.columns.get_loc(ham_tc)

    if "GSM" not in merged.columns:
        merged.insert(tc_index + 1, "GSM", merged[tel_col])
    else:
        merged["GSM"] = merged[tel_col]

    # --- geçici TEL kolonunu sil ---
    merged.drop(columns=[tel_col], inplace=True)

    # --- kaydet ---
    merged.to_excel(output_path, index=False)
    return output_path
