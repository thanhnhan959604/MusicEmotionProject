import os
import re
import ast

import pandas as pd

from src.utils.config import PipelineConfig
from src.utils.logger import get_logger

# 13 đặc trưng âm thanh
AUDIO_FEATURE_COLS = [
    "danceability", "energy", "key", "loudness", "mode",
    "speechiness", "acousticness", "instrumentalness",
    "liveness", "valence", "tempo", "duration_ms", "time_signature",
]

# regex ký tự tiếng Việt có dấu
VN_CHARS = re.compile(
    r"[àáạảãâầấậẩẫăằắặẳẵèéẹẻẽêềếệểễ"
    r"ìíịỉĩòóọỏõôồốộổỗơờớợởỡùúụủũưừứựửữ"
    r"ỳýỵỷỹđ]",
    re.IGNORECASE,
)

# helpers
def parse_artists(raw):
    # chuyển cột 'artists' từ chuỗi json-like về tên ca sĩ thuần.
    if not isinstance(raw, str) or not raw.strip():
        return ""

    try:
        parsed = ast.literal_eval(raw)
        if isinstance(parsed, list):
            names = []
            for item in parsed:
                if isinstance(item, dict):
                    name = item.get("name") or item.get("profile", {}).get("name")
                elif isinstance(item, str):
                    name = item
                else:
                    name = None
                if name:
                    names.append(name.strip())
            return ", ".join(names)
    except Exception:
        pass

    return raw.strip()


def has_vietnamese(text):

    return bool(VN_CHARS.search(str(text)))


def is_known_viet_artist(artist_str):

    for name in artist_str.split(","):
        if name.strip().lower() in PipelineConfig.KNOWN_VIET_ARTISTS:
            return True
    return False


def load_csv(filepath, label, logger):

    if not os.path.exists(filepath):
        raise FileNotFoundError(
            f"Khong tim thay '{filepath}'. "
            f"Hay chay step_02_fetch_metadata_features.py truoc."
        )
    try:
        df = pd.read_csv(filepath, low_memory=False)
        logger.info(f"[{label}] Doc OK: {len(df):,} dong | {len(df.columns)} cot.")
        return df
    except Exception as e:
        logger.error(f"[{label}] Khong doc duoc file: {e}")
        raise


def safe_makedirs(filepath):

    dirpath = os.path.dirname(filepath)
    if dirpath:
        os.makedirs(dirpath, exist_ok=True)

# logic xử lý chính
def step_merge(df_meta, df_feat, logger):

    if "id" in df_meta.columns:
        df_meta = df_meta.rename(columns={"id": "Spotify_ID"})

    df_meta["Spotify_ID"] = df_meta["Spotify_ID"].astype(str).str.strip()
    df_feat["Spotify_ID"] = df_feat["Spotify_ID"].astype(str).str.strip()

    before = len(df_meta)
    df = pd.merge(df_meta, df_feat, on="Spotify_ID", how="left", suffixes=("", "_feat"))

    # xoá các cột trùng từ features
    dup_cols = [c for c in df.columns if c.endswith("_feat")]
    if dup_cols:
        df.drop(columns=dup_cols, inplace=True)
        logger.info(f"[merge] Da bo {len(dup_cols)} cot trung: {dup_cols}")

    logger.info(
        f"[merge] {before:,} metadata x {len(df_feat):,} features "
        f"-> {len(df):,} dong sau left-join."
    )
    return df


def step_drop_invalid(df, logger):

    before = len(df)

    name_col = next((c for c in ["name", "Track_Name"] if c in df.columns), None)
    required = ["Spotify_ID"] + ([name_col] if name_col else [])
    df = df.dropna(subset=required)

    # xoá id có giá trị chuỗi 'nan' hoặc toàn khoảng trắng
    df = df[df["Spotify_ID"].str.lower() != "nan"]
    df = df[df["Spotify_ID"].str.strip().str.len() > 0]

    logger.info(
        f"[drop_invalid] {before:,} -> {len(df):,} "
        f"(da xoa {before - len(df):,} ban ghi thieu ID/ten)."
    )
    return df


def step_dedup_id(df, logger):

    before = len(df)
    df = df.drop_duplicates(subset=["Spotify_ID"], keep="first")
    logger.info(
        f"[dedup_id] {before:,} -> {len(df):,} "
        f"(da xoa {before - len(df):,} bai trung ID)."
    )
    return df


def step_parse_artists(df, logger):

    raw_col = next((c for c in ["artists", "Artist"] if c in df.columns), None)

    if raw_col is None:
        logger.warning("[parse_artists] Khong tim thay cot artists. Tao cot Artist = 'Unknown'.")
        df["Artist"] = "Unknown"
        return df

    df["Artist"] = df[raw_col].apply(parse_artists)

    if raw_col != "Artist":
        df.drop(columns=[raw_col], inplace=True)
        logger.info(f"[parse_artists] Parse xong '{raw_col}' thanh 'Artist'.")
    else:
        logger.info("[parse_artists] Da parse lai cot 'Artist' tai cho.")

    return df


def step_filter_vietnamese(df, logger):

    before = len(df)

    name_col = next((c for c in ["name", "Track_Name"] if c in df.columns), None)

    # điều kiện A: regex ký tự có dấu
    text_series = df["Artist"].astype(str)
    if name_col:
        text_series = text_series + " " + df[name_col].astype(str)
    mask_vn_chars = text_series.apply(has_vietnamese)

    # điều kiện B: whitelist nghệ sĩ Việt tên không dấu
    mask_whitelist = df["Artist"].astype(str).apply(is_known_viet_artist)

    # giữ nếu thoả 1 trong 2 điều kiện
    df = df[mask_vn_chars | mask_whitelist].copy()

    kept_by_whitelist_only = (mask_whitelist & ~mask_vn_chars).sum()

    logger.info(
        f"[filter_vn] {before:,} -> {len(df):,} "
        f"(da loai {before - len(df):,} bai | "
        f"whitelist giu them {kept_by_whitelist_only} bai ten tieng Anh)."
    )
    return df


def step_dedup_name(df, logger):

    before = len(df)

    name_col = next((c for c in ["name", "Track_Name"] if c in df.columns), None)
    if name_col is None:
        logger.warning("[dedup_name] Khong co cot ten bai. Bo qua buoc nay.")
        return df

    df["_tmp_name"]   = df[name_col].astype(str).str.lower().str.strip()
    df["_tmp_artist"] = df["Artist"].astype(str).str.lower().str.strip()

    df = df.drop_duplicates(subset=["_tmp_name", "_tmp_artist"], keep="first")
    df = df.drop(columns=["_tmp_name", "_tmp_artist"])

    logger.info(
        f"[dedup_name] {before:,} -> {len(df):,} "
        f"(da xoa {before - len(df):,} bai trung ten + nghe si)."
    )
    return df


def step_select_and_rename(df, logger):
    """
    Chuan hoa ten cot va chat loc nhung cot can thiet cho cac buoc sau.

    Cot dau ra:
        Spotify_ID, Track_Name, Artist, Popularity, Duration_MS,
        Preview_Audio_URL, Release_Date  +  13 audio features (neu co)
    """
    rename_map = {
        "name"               : "Track_Name",
        "popularity"         : "Popularity",
        "duration_ms"        : "Duration_MS",
        "preview_url"        : "Preview_Audio_URL",
        "album.release_date" : "Release_Date",
    }

    existing_rename = {k: v for k, v in rename_map.items() if k in df.columns}
    df = df.rename(columns=existing_rename)

    core_cols = [
        "Spotify_ID", "Track_Name", "Artist",
        "Popularity", "Duration_MS", "Preview_Audio_URL", "Release_Date",
    ]
    feature_cols = [c for c in AUDIO_FEATURE_COLS if c in df.columns]

    keep = [c for c in core_cols if c in df.columns] + feature_cols
    df = df[keep].copy()

    if "Popularity" in df.columns:
        df["Popularity"] = pd.to_numeric(df["Popularity"], errors="coerce").astype("float32")

    if "Duration_MS" in df.columns:
        df["Duration_MS"] = pd.to_numeric(df["Duration_MS"], errors="coerce").astype("float32")

    logger.info(
        f"[select_rename] Giu lai {len(df.columns)} cot | "
        f"{len(feature_cols)}/13 audio features co mat."
    )
    return df

# entry point

def main():

    logger = get_logger("Step03_CleanAndDedup", "step3.log")

    meta_file = str(PipelineConfig.RAW_METADATA_FILE)
    feat_file = str(PipelineConfig.AUDIO_FEATURES_FILE)
    out_file  = str(PipelineConfig.CLEANED_DATA_FILE)

    logger.info("=" * 55)
    logger.info("BUOC 3: LAM SACH & LOAI BO TRUNG LAP")
    logger.info("=" * 55)
    logger.info(f"[-] Metadata dau vao : {meta_file}")
    logger.info(f"[-] Features dau vao : {feat_file}")
    logger.info(f"[-] Ket qua dau ra   : {out_file}")
    logger.info(f"[-] Whitelist nghe si: {len(PipelineConfig.KNOWN_VIET_ARTISTS)} nghe si")
    logger.info("-" * 55)

    # đọc dữ liệu
    df_meta = load_csv(meta_file, "metadata", logger)
    df_feat = load_csv(feat_file, "audio_features", logger)

    # Pipeline xử lý tuần tự
    df = step_merge(df_meta, df_feat, logger)
    df = step_drop_invalid(df, logger)
    df = step_dedup_id(df, logger)
    df = step_parse_artists(df, logger)
    df = step_filter_vietnamese(df, logger)
    df = step_dedup_name(df, logger)
    df = step_select_and_rename(df, logger)

    # Lưu kết quả
    safe_makedirs(out_file)
    df.to_csv(out_file, index=False, encoding="utf-8-sig")

    logger.info("=" * 55)
    logger.info("[THANH CONG] HOAN TAT BUOC 3")
    logger.info(f"[-] Tong bai hat sach : {len(df):,}")
    logger.info(f"[-] So cot            : {len(df.columns)}")
    logger.info(f"[-] File luu tai      : {out_file}")
    logger.info("=" * 55)


if __name__ == "__main__":
    try:
        main()
    except FileNotFoundError as e:
        print(f"\n[!] Loi: {e}")
    except KeyboardInterrupt:
        print("\n\n[!] Da dung chuong trinh. Khong co du lieu nao bi mat.")
    except Exception as e:
        print(f"\n[!] Loi khong mong muon: {e}")
        raise