import os
import shutil
import pandas as pd
from src.utils.config import PipelineConfig
from src.utils.logger import get_logger

# helpers - I/O

def load_csv(filepath, label, logger):
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
    dirpath = os.path.dirname(filepath)
    if dirpath:
        os.makedirs(dirpath, exist_ok=True)

# helpers - Kiểm tra audio

def scan_audio_dir(audio_dir, logger):
    if not os.path.exists(audio_dir):
        raise FileNotFoundError(
            f"Không tìm thấy thư mục audio '{audio_dir}'. "
            "Hãy chạy step_06_download_audio_preview.py trước."
        )

    available_ids = set()
    total_size_mb = 0.0
    empty_count = 0

    for filename in os.listdir(audio_dir):
        if not filename.endswith(".mp3"):
            continue

        filepath = os.path.join(audio_dir, filename)
        file_size = os.path.getsize(filepath)

        if file_size == 0:
            empty_count += 1
            logger.warning(f"[SCAN] Bỏ qua file rỗng: {filename}")
            continue

        track_id = filename.replace(".mp3", "").strip()
        available_ids.add(track_id)
        total_size_mb += file_size / (1024 * 1024)

    logger.info(
        f"[SCAN] Tìm thấy {len(available_ids):,} file MP3 hợp lệ "
        f"| File rỗng bị bỏ qua: {empty_count} "
        f"| Tổng dung lượng: {total_size_mb:.1f} MB."
    )
    return available_ids

# helpers - Di chuyển audio

def move_audio_files(spotify_ids, src_audio_dir, dst_audio_dir, logger):
    if not spotify_ids:
        logger.info("[MOVE AUDIO] Không có file nào cần di chuyển.")
        return

    os.makedirs(dst_audio_dir, exist_ok=True)

    moved_count = 0
    not_found_count = 0

    for track_id in spotify_ids:
        src_path = os.path.join(src_audio_dir, f"{track_id}.mp3")
        dst_path = os.path.join(dst_audio_dir, f"{track_id}.mp3")

        if os.path.exists(src_path):
            try:
                shutil.move(src_path, dst_path)
                moved_count += 1
                logger.debug(f"[MOVE AUDIO] {src_path} -> {dst_path}")
            except Exception as err:
                logger.warning(f"[MOVE AUDIO] Không di chuyển được '{src_path}': {err}")
        else:
            not_found_count += 1

    logger.info(
        f"[MOVE AUDIO] Đã di chuyển {moved_count:,} file MP3 sang '{dst_audio_dir}' "
        f"| Không tìm thấy: {not_found_count:,} file."
    )

# các bước xử lý

def step_normalize_id(df, logger):
    df = df.copy()
    df["Spotify_ID"] = df["Spotify_ID"].astype(str).str.strip()

    before = len(df)
    df = df[
        df["Spotify_ID"].str.len().gt(0)
        & df["Spotify_ID"].str.lower().ne("nan")
    ]
    removed = before - len(df)

    if removed:
        logger.info(f"[NORMALIZE ID] Loại bỏ {removed:,} dòng có ID không hợp lệ.")
    else:
        logger.info("[NORMALIZE ID] Tất cả ID đều hợp lệ.")
    return df


def step_sync_audio(df, available_ids, logger):
    before = len(df)

    df_synced = df[df["Spotify_ID"].isin(available_ids)].copy()
    df_missing = df[~df["Spotify_ID"].isin(available_ids)].copy()

    removed = before - len(df_synced)

    logger.info(
        f"[SYNC AUDIO] {before:,} -> {len(df_synced):,} bài giữ lại "
        f"(loại {removed:,} bài thiếu file MP3)."
    )
    return df_synced, df_missing


def step_validate_completeness(df, audio_dir, quarantine_audio_dir, quarantine_csv, logger):
    # dòng nào có BẤT KỲ ô NaN nào -> loại bài đó + chuyển audio sang quarantine.
    before = len(df)

    # log chi tiết từng cột còn NaN để dễ debug
    for col in df.columns:
        null_count = df[col].isna().sum()
        if null_count:
            logger.warning(f"[VALIDATE] Cột '{col}' có {null_count:,} giá trị NaN.")

    # một dòng chỉ cần 1 ô NaN là bị loại
    mask_invalid = df.isna().any(axis=1)
    df_invalid = df[mask_invalid].copy()
    df_valid = df[~mask_invalid].copy()

    removed = before - len(df_valid)

    if removed:
        logger.info(
            f"[VALIDATE] Loại {removed:,} bài có ít nhất 1 ô NaN. "
            f"Còn lại: {len(df_valid):,} bài."
        )

        # lưu dòng bị loại ra CSV quarantine
        safe_makedirs(quarantine_csv)
        df_invalid.to_csv(quarantine_csv, index=False, encoding="utf-8-sig")
        logger.info(f"[VALIDATE] Đã lưu {removed:,} dòng bị loại vào '{quarantine_csv}'.")

        # di chuyển MP3 liên quan sang folder quarantine
        ids_to_move = df_invalid["Spotify_ID"].tolist()
        move_audio_files(ids_to_move, audio_dir, quarantine_audio_dir, logger)
    else:
        logger.info("[VALIDATE] Tất cả dòng đều đầy đủ, không có bài nào bị loại.")

    logger.info(
        f"[VALIDATE] Hoàn tất. Dataset có {len(df_valid):,} bài x {len(df_valid.columns)} cột."
    )
    return df_valid


def step_drop_empty_cols(df, logger):
    before_cols = set(df.columns)
    df = df.dropna(axis=1, how="all")
    dropped_names = before_cols - set(df.columns)

    if dropped_names:
        logger.info(
            f"[DROP EMPTY COLS] Đã xoá {len(dropped_names)} cột: {sorted(dropped_names)}. "
            f"Còn lại: {len(df.columns)} cột."
        )
    else:
        logger.info(f"[DROP EMPTY COLS] Không có cột nào trống. Còn lại: {len(df.columns)} cột.")
    return df


def step_reorder_columns(df, logger):
    priority_cols = [
        "Spotify_ID",
        "Track_Name",
        "Artist",
        "Lyrics",
        "Popularity",
        "Duration_MS",
        "Preview_Audio_URL",
        "Release_Date",
    ]
    audio_feature_cols = [
        "danceability", "energy", "key", "loudness", "mode",
        "speechiness", "acousticness", "instrumentalness",
        "liveness", "valence", "tempo", "duration_ms", "time_signature",
    ]

    existing_priority = [c for c in priority_cols if c in df.columns]
    existing_features = [c for c in audio_feature_cols if c in df.columns]
    remainder = [
        c for c in df.columns
        if c not in existing_priority and c not in existing_features
    ]

    ordered = existing_priority + existing_features + remainder
    df = df[ordered]

    logger.info(
        f"[REORDER] Thứ tự cột: {', '.join(ordered[:5])} ... "
        f"(tổng {len(ordered)} cột | {len(existing_features)}/13 audio features)."
    )
    return df


def save_missing_log(df_missing, log_dir, logger):
    if df_missing.empty:
        logger.info("[MISSING LOG] Không có bài nào bị loại. Không cần ghi log.")
        return

    log_path = os.path.join(log_dir, "step7_missing_audio.csv")
    safe_makedirs(log_path)

    cols_to_keep = [
        c for c in ["Spotify_ID", "Track_Name", "Artist", "Preview_Audio_URL"]
        if c in df_missing.columns
    ]
    df_missing[cols_to_keep].to_csv(log_path, index=False, encoding="utf-8-sig")

    logger.info(
        f"[MISSING LOG] Đã ghi {len(df_missing):,} bài thiếu MP3 vào '{log_path}'."
    )


def log_summary(df, out_file, logger):
    audio_feature_cols = [
        "danceability", "energy", "key", "loudness", "mode",
        "speechiness", "acousticness", "instrumentalness",
        "liveness", "valence", "tempo", "duration_ms", "time_signature",
    ]

    has_lyrics = int(df["Lyrics"].notna().sum()) if "Lyrics" in df.columns else 0
    audio_feat_count = len([c for c in audio_feature_cols if c in df.columns])

    logger.info("=" * 55)
    logger.info("[THÀNH CÔNG] HOÀN TẤT BƯỚC 7 - TRAIN READY")
    logger.info(f"[-] Tổng bài TRAIN-READY: {len(df):,}")
    logger.info(f"[-] Bài có Lyrics: {has_lyrics:,}")
    logger.info(f"[-] Audio features có mặt: {audio_feat_count}/13")
    logger.info(f"[-] Số cột: {len(df.columns)}")
    logger.info(f"[-] File lưu tại: {out_file}")
    logger.info("=" * 55)

# entry point

def main():
    logger = get_logger("Step07_SyncAndTrainReady", "step7.log")

    master_file = str(PipelineConfig.MASTER_DATASET_FILE)
    audio_dir = str(PipelineConfig.AUDIO_DIR)
    out_file = str(PipelineConfig.TRAIN_READY_FILE)
    log_dir = str(PipelineConfig.DATA_DIR)
    quarantine_audio_dir = str(PipelineConfig.QUARANTINE_AUDIO_DIR)
    quarantine_csv = str(PipelineConfig.QUARANTINE_CSV)

    logger.info("=" * 55)
    logger.info("BUOC 7: ĐỒNG BỘ AUDIO -> TRAIN READY")
    logger.info("=" * 55)
    logger.info(f"[-] Master dataset (Bước 5): {master_file}")
    logger.info(f"[-] Thư mục audio (Bước 6): {audio_dir}")
    logger.info(f"[-] Train-ready output: {out_file}")
    logger.info(f"[-] Quarantine audio: {quarantine_audio_dir}")
    logger.info(f"[-] Quarantine CSV: {quarantine_csv}")
    logger.info("-" * 55)

    df = load_csv(master_file, "master", logger)

    available_ids = scan_audio_dir(audio_dir, logger)

    df = step_normalize_id(df, logger)

    df, df_missing = step_sync_audio(df, available_ids, logger)

    # xoá cột toàn NaN trước, sau đó mới lọc dòng
    # (tránh cột phụ không quan trọng làm loại oan bài hát)
    df = step_drop_empty_cols(df, logger)

    # dòng nào còn NaN ở bất kỳ ô nào -> loại bài + chuyển audio
    df = step_validate_completeness(
        df, audio_dir,
        quarantine_audio_dir, quarantine_csv,
        logger,
    )

    df = step_reorder_columns(df, logger)

    save_missing_log(df_missing, log_dir, logger)

    safe_makedirs(out_file)
    df.to_csv(out_file, index=False, encoding="utf-8-sig")

    log_summary(df, out_file, logger)


if __name__ == "__main__":
    try:
        main()
    except FileNotFoundError as err:
        print(f"\n[!] Lỗi: {err}")
    except ValueError as err:
        print(f"\n[!] Lỗi dữ liệu: {err}")
    except KeyboardInterrupt:
        print(
            "\n\n[!] ÉP DỪNG CHƯƠNG TRÌNH (Ctrl+C). "
            "Không có dữ liệu nào bị mất."
        )
    except Exception as err:
        print(f"\n[!] Lỗi không mong muốn: {err}")
        raise