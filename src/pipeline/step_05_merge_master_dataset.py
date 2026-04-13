import os

import pandas as pd

from src.utils.config import PipelineConfig
from src.utils.logger import get_logger

# Hằng số

# 13 đặc trưng âm thanh
AUDIO_FEATURE_COLS = [
    "danceability", "energy", "key", "loudness", "mode",
    "speechiness", "acousticness", "instrumentalness",
    "liveness", "valence", "tempo", "duration_ms", "time_signature",
]

# thứ tự các cột ưu tiên hiển thị đầu file CSV
PRIORITY_COLS = [
    "Spotify_ID",
    "Track_Name",
    "Artist",
    "Lyrics",
    "Popularity",
    "Duration_MS",
    "Preview_Audio_URL",
    "Release_Date",
]

# Helpers - I/O

def load_csv(filepath, label, logger):
    # đọc và kiểm tra tồn tại.
    if not os.path.exists(filepath):
        raise FileNotFoundError(
            f"Không tìm thấy '{filepath}'. "
            f"Hãy chạy bước trước để tạo file này."
        )
    try:
        df = pd.read_csv(filepath, low_memory=False)
        logger.info(f"[{label}] Đọc OK: {len(df):,} dòng | {len(df.columns)} cột.")
        return df
    except Exception as err:
        logger.error(f"[{label}] Không đọc được file: {err}")
        raise


def safe_makedirs(filepath):
    # tạo thư mục cha nếu chưa tồn tại.
    dirpath = os.path.dirname(filepath)
    if dirpath:
        os.makedirs(dirpath, exist_ok=True)


# Helper - chuẩn hoá đầu vào

def normalize_id_col(df):
    # ép chuỗi + xoá khoảng trắng, trả về bản copy để không mutate gốc.
    df = df.copy()
    df["Spotify_ID"] = df["Spotify_ID"].astype(str).str.strip()
    return df


# Các bước xử lý

def step_validate_inputs(df_meta, df_lyrics, logger):
    # kiểm tra cấu trúc trước khi gộp.
    required_meta   = {"Spotify_ID", "Track_Name", "Artist"}
    required_lyrics = {"Spotify_ID", "Lyrics"}

    missing_meta   = required_meta   - set(df_meta.columns)
    missing_lyrics = required_lyrics - set(df_lyrics.columns)

    if missing_meta:
        raise ValueError(
            f"[validate] File metadata thiếu cột: {missing_meta}. "
            "Hãy kiểm tra lại kết quả Bước 3."
        )
    if missing_lyrics:
        raise ValueError(
            f"[validate] File lyrics thiếu cột: {missing_lyrics}. "
            "Hãy kiểm tra lại kết quả Bước 4."
        )

    logger.info("[validate] Cấu trúc hai file đầu vào hợp lệ.")


def step_dedup_inputs(df_meta, df_lyrics, logger):
    # xoá trùng ID trong từng file trước khi gộp.
    before_meta   = len(df_meta)
    before_lyrics = len(df_lyrics)

    df_meta   = df_meta.drop_duplicates(subset=["Spotify_ID"], keep="first")
    df_lyrics = df_lyrics.drop_duplicates(subset=["Spotify_ID"], keep="first")

    dropped_meta   = before_meta   - len(df_meta)
    dropped_lyrics = before_lyrics - len(df_lyrics)

    logger.info(
        f"[dedup_inputs] Metadata : {before_meta:,} -> {len(df_meta):,} "
        f"(xoá {dropped_meta:,} dòng trùng ID)."
    )
    logger.info(
        f"[dedup_inputs] Lyrics   : {before_lyrics:,} -> {len(df_lyrics):,} "
        f"(xoá {dropped_lyrics:,} dòng trùng ID)."
    )
    return df_meta, df_lyrics


def step_merge(df_meta, df_lyrics, logger):
    # dùng inner-join để kết hợp metadata + lyrics (step_03 + step_04).
    n_meta   = len(df_meta)
    n_lyrics = len(df_lyrics)

    df = pd.merge(df_meta, df_lyrics, on="Spotify_ID", how="inner")

    logger.info(
        f"[merge] {n_meta:,} metadata x {n_lyrics:,} lyrics "
        f"-> {len(df):,} bài sau inner-join."
    )

    # tính chính xác số bài bị loại bằng set difference
    merged_ids     = set(df["Spotify_ID"])
    only_in_meta   = len(set(df_meta["Spotify_ID"])   - merged_ids)
    only_in_lyrics = len(set(df_lyrics["Spotify_ID"]) - merged_ids)

    logger.info(
        f"[merge] Bị loại: {only_in_meta:,} bài chỉ có metadata (không có lyrics VI) | "
        f"{only_in_lyrics:,} bài chỉ có lyrics (không qua Bước 3)."
    )
    return df


def step_clean_lyrics(df, logger):
    # loại bỏ các bài Lyrics trống hoặc 'No Lyrics' còn sót lại sau Bước 4.
    before = len(df)
    mask_bad = (
        df["Lyrics"].isna()
        | df["Lyrics"].astype(str).str.strip().eq("")
        | df["Lyrics"].astype(str).str.strip().eq("No Lyrics")
    )
    df = df[~mask_bad].copy()

    removed = before - len(df)
    if removed:
        logger.info(
            f"[clean_lyrics] Loại thêm {removed:,} bài có Lyrics trống / 'No Lyrics'."
        )
    else:
        logger.info("[clean_lyrics] Không có bài nào còn Lyrics trống. Tốt!")
    return df


def step_drop_empty_cols(df, logger):
    # xoá các cột không mang thông tin (toàn bộ NaN).
    before_cols = set(df.columns)
    df = df.dropna(axis=1, how="all")
    dropped_names = before_cols - set(df.columns)

    if dropped_names:
        logger.info(
            f"[drop_empty_cols] Đã xoá {len(dropped_names)} cột: {sorted(dropped_names)}. "
            f"Còn lại: {len(df.columns)} cột."
        )
    else:
        logger.info(f"[drop_empty_cols] Không có cột nào trống. Còn lại: {len(df.columns)} cột.")
    return df


def step_cast_numerics(df, logger):
    # ép kiểu số cho các cột để tránh chuỗi 'nan' lẫn vào.
    df = df.copy()

    int_cols = ["key", "mode", "time_signature"]
    float_cols = [
        "danceability", "energy", "loudness", "speechiness",
        "acousticness", "instrumentalness", "liveness",
        "valence", "tempo", "duration_ms",
        "Popularity", "Duration_MS",
    ]

    for col in float_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").astype("float32")

    for col in int_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    logger.info("[cast_numerics] Đã ép kiểu số cho Popularity, Duration và audio features.")
    return df


def step_final_dedup(df, logger):
    # xoá trùng theo Track_Name + Artist để đảm bảo dataset thuần nhất.
    before = len(df)

    if "Track_Name" not in df.columns or "Artist" not in df.columns:
        logger.warning("[final_dedup] Không có cột Track_Name / Artist. Bỏ qua bước này.")
        return df

    df["_tmp_name"]   = df["Track_Name"].astype(str).str.lower().str.strip()
    df["_tmp_artist"] = df["Artist"].astype(str).str.lower().str.strip()
    df = df.drop_duplicates(subset=["_tmp_name", "_tmp_artist"], keep="first")
    df = df.drop(columns=["_tmp_name", "_tmp_artist"])

    logger.info(
        f"[final_dedup] {before:,} -> {len(df):,} "
        f"(xoá {before - len(df):,} bài trùng Tên + Nghệ sĩ)."
    )
    return df


def step_reorder_columns(df, logger):
    # sắp xếp các cột theo đúng thứ tự: ưu tiên -> audio features -> còn lại.
    existing_priority = [c for c in PRIORITY_COLS      if c in df.columns]
    audio_features    = [c for c in AUDIO_FEATURE_COLS if c in df.columns]
    remainder         = [
        c for c in df.columns
        if c not in existing_priority and c not in audio_features
    ]

    ordered = existing_priority + audio_features + remainder
    df = df[ordered]

    logger.info(
        f"[reorder] Thứ tự cột: {', '.join(ordered[:6])} ... "
        f"(tổng {len(ordered)} cột | {len(audio_features)}/13 audio features)."
    )
    return df


# In báo cáo tổng kết sau khi hoàn tất bước 5

def log_summary(df, out_file, logger):
    audio_present = [c for c in AUDIO_FEATURE_COLS if c in df.columns]
    has_preview   = int(df["Preview_Audio_URL"].notna().sum()) if "Preview_Audio_URL" in df.columns else 0

    logger.info("=" * 55)
    logger.info("[THÀNH CÔNG] HOÀN TẤT BƯỚC 5")
    logger.info(f"[-] Tổng bài hát master   : {len(df):,}")
    logger.info(f"[-] Số cột                : {len(df.columns)}")
    logger.info(f"[-] Audio features có mặt : {len(audio_present)}/13")
    logger.info(f"[-] Bài có link Preview   : {has_preview:,}")
    logger.info(f"[-] File lưu tại          : {out_file}")
    logger.info("=" * 55)


# Entry point

def main():
    logger = get_logger("Step05_MergeMasterDataset", "step5.log")

    meta_file   = str(PipelineConfig.CLEANED_DATA_FILE)     # Bước 3
    lyrics_file = str(PipelineConfig.VIETNAMESE_ONLY_FILE)  # Bước 4
    out_file    = str(PipelineConfig.MASTER_DATASET_FILE)   # Bước 5 output

    logger.info("=" * 55)
    logger.info("BUOC 5: GOP DU LIEU -> MASTER DATASET")
    logger.info("=" * 55)
    logger.info(f"[-] Metadata (Buoc 3)  : {meta_file}")
    logger.info(f"[-] Lyrics VI (Buoc 4) : {lyrics_file}")
    logger.info(f"[-] Master output      : {out_file}")
    logger.info("-" * 55)

    # đọc dữ liệu đầu vào
    df_meta   = load_csv(meta_file,   "metadata",  logger)
    df_lyrics = load_csv(lyrics_file, "lyrics_vi", logger)

    # chuẩn hoá cột ID
    df_meta   = normalize_id_col(df_meta)
    df_lyrics = normalize_id_col(df_lyrics)

    # pipeline xử lý tuần tự
    step_validate_inputs(df_meta, df_lyrics, logger)

    df_meta, df_lyrics = step_dedup_inputs(df_meta, df_lyrics, logger)

    df = step_merge(df_meta, df_lyrics, logger)
    df = step_clean_lyrics(df, logger)
    df = step_drop_empty_cols(df, logger)
    df = step_cast_numerics(df, logger)
    df = step_final_dedup(df, logger)
    df = step_reorder_columns(df, logger)

    # lưu kết quả
    safe_makedirs(out_file)
    df.to_csv(out_file, index=False, encoding="utf-8-sig")

    log_summary(df, out_file, logger)


if __name__ == "__main__":
    try:
        main()
    except FileNotFoundError as err:
        print(f"\n[!] Loi: {err}")
    except ValueError as err:
        print(f"\n[!] Loi du lieu: {err}")
    except KeyboardInterrupt:
        print(
            "\n\n[!] EP DUNG CHUONG TRINH (Ctrl+C). "
            "Khong co du lieu nao bi mat."
        )
    except Exception as err:
        print(f"\n[!] Loi khong mong muon: {err}")
        raise