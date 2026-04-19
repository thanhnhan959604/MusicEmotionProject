import os
import re
import time
import pandas as pd
from langdetect import detect, DetectorFactory

from src.utils.config import PipelineConfig
from src.utils.logger import get_logger
from src.utils.http_client import Spotify81Client

# cố định seed để nhận diện ngôn ngữ nhất quán
DetectorFactory.seed = 0

# hằng số
CHUNK_SIZE = 10
SLEEP_OK = 1.5
SLEEP_RETRY = 15.0
SLEEP_NET = 5.0
MAX_RETRIES = 3

ENDPOINT_LYRICS = "/track_lyrics_batch"

# TỪ KHOÁ NHẬN DIỆN LIÊN KHÚC
# Từ khoá trong TÊN BÀI — chỉ cần xuất hiện là loại
LIENKHUC_NAME_KEYWORDS = PipelineConfig.LIENKHUC_NAME_KEYWORDS

# Pattern trong LYRICS — dấu hiệu ghép nhiều bài
# (Xuất hiện ≥ 3 lần tiêu đề bài khác nhau trong cùng 1 file lời)
LIENKHUC_LYRICS_TITLE_PATTERN = re.compile(
    r"^\s*[\(\[【].*?[\)\]】]\s*$",   # dòng chỉ chứa tên bài trong ngoặc
    re.MULTILINE,
)


def is_lienkhuc(track_name: str, lyrics: str) -> bool:
    name_lower = str(track_name).lower().strip()

    # Tầng 1: tên bài
    for kw in LIENKHUC_NAME_KEYWORDS:
        if kw in name_lower:
            return True

    # Tầng 2 & 3: dựa vào lyrics
    lyrics_str = str(lyrics).strip()
    if lyrics_str and lyrics_str != "No Lyrics":
        # Tầng 2: nhiều tiêu đề bài trong ngoặc
        title_markers = LIENKHUC_LYRICS_TITLE_PATTERN.findall(lyrics_str)
        if len(title_markers) >= 3:
            return True

        # Tầng 3: lyrics quá dài (> 6 000 ký tự)
        if len(lyrics_str) > 6000:
            return True

    return False

# HELPERS - ĐỌC / GHI
def load_input_ids(input_file, logger):
    if not os.path.exists(input_file):
        raise FileNotFoundError(
            f"Không tìm thấy '{input_file}'. "
            "Hãy chạy `step_03_clean_and_dedup.py` trước."
        )

    df = pd.read_csv(input_file, low_memory=False)

    if "Spotify_ID" not in df.columns:
        raise ValueError(f"File '{input_file}' không có cột 'Spotify_ID'.")

    ids = (
        df["Spotify_ID"]
        .dropna()
        .astype(str)
        .str.strip()
        .tolist()
    )
    ids = [tid for tid in ids if tid and tid.lower() != "nan"]
    ids = list(dict.fromkeys(ids))

    logger.info(f"[load_ids] Đọc OK: {len(ids):,} IDs hợp lệ từ '{input_file}'.")
    return ids


def load_fetched_ids(output_file, logger):
    if not os.path.exists(output_file):
        return set()

    try:
        df = pd.read_csv(output_file, low_memory=False, usecols=["Spotify_ID"])
        fetched = set(df["Spotify_ID"].astype(str).str.strip().tolist())
        logger.info(f"[resume] Đã có {len(fetched):,} IDs. Bỏ qua khi chạy lại.")
        return fetched
    except Exception as err:
        logger.warning(f"[resume] Không đọc được file cũ: {err}. Bắt đầu từ đầu.")
        return set()


def safe_makedirs(filepath):
    dirpath = os.path.dirname(filepath)
    if dirpath:
        os.makedirs(dirpath, exist_ok=True)


def append_to_csv(df, output_file):
    write_header = not os.path.exists(output_file)
    df.to_csv(
        output_file,
        mode="a",
        index=False,
        header=write_header,
        encoding="utf-8-sig",
    )


# PHA A — TẢI LYRICS
def extract_single_lyric(item):
    track_id = item.get("id")
    clean_text = "No Lyrics"

    if item.get("error"):
        return track_id, clean_text

    lyrics_data = item.get("lyrics")
    if lyrics_data and isinstance(lyrics_data, dict) and "lines" in lyrics_data:
        lines = lyrics_data["lines"]
        words_list = [
            line.get("words", "").strip()
            for line in lines
            if line.get("words", "").strip()
        ]
        if words_list:
            clean_text = "\n".join(words_list)

    return track_id, clean_text


def fetch_lyrics_batch(client, chunk_ids, batch_num, logger):
    querystring = {"ids": ",".join(chunk_ids), "format": "json"}

    for attempt in range(1, MAX_RETRIES + 1):
        data = client.get(ENDPOINT_LYRICS, params=querystring)

        if data is None:
            logger.warning(
                f"[lyrics] Mẻ {batch_num} - lần {attempt}/{MAX_RETRIES}: "
                "client trả về None, thử lại sau 5 giây..."
            )
            time.sleep(SLEEP_NET)
            continue

        if isinstance(data, dict):
            items_list = data.get("results", [])
        elif isinstance(data, list):
            items_list = data
        else:
            logger.warning(f"[lyrics] Mẻ {batch_num}: Kiểu response lạ ({type(data)}). Bỏ qua.")
            break

        batch_rows = []
        for item in items_list:
            if not item or not isinstance(item, dict):
                continue
            track_id, lyrics_text = extract_single_lyric(item)
            if track_id:
                batch_rows.append({"Spotify_ID": str(track_id), "Lyrics": lyrics_text})

        # bù những ID bị API bỏ sót
        found_ids = {row["Spotify_ID"] for row in batch_rows}
        for tid in chunk_ids:
            if str(tid) not in found_ids:
                batch_rows.append({"Spotify_ID": str(tid), "Lyrics": "No Lyrics"})

        logger.info(f"[lyrics] Mẻ {batch_num}: [OK] {len(batch_rows)}/{len(chunk_ids)} bài.")
        time.sleep(SLEEP_OK)
        return batch_rows

    logger.error(f"[lyrics] Mẻ {batch_num}: [LỖI] Thất bại sau {MAX_RETRIES} lần.")
    return [{"Spotify_ID": str(tid), "Lyrics": "No Lyrics"} for tid in chunk_ids]


def phase_a_fetch_lyrics(client, all_ids, output_file, logger):
    fetched_ids = load_fetched_ids(output_file, logger)
    ids_to_fetch = [tid for tid in all_ids if tid not in fetched_ids]

    logger.info("PHA A - TẢI LYRICS")
    logger.info(f"[-] Tổng IDs: {len(all_ids):,}")
    logger.info(f"[-] Đã có (resume): {len(fetched_ids):,}")
    logger.info(f"[-] Cần tải thêm: {len(ids_to_fetch):,}")

    if not ids_to_fetch:
        logger.info("[PHA A] Đã hoàn tất trước đó. Bỏ qua.")
        return

    safe_makedirs(output_file)
    total_batches = (len(ids_to_fetch) + CHUNK_SIZE - 1) // CHUNK_SIZE
    success_count = fail_count = 0

    for i in range(0, len(ids_to_fetch), CHUNK_SIZE):
        chunk_ids = ids_to_fetch[i: i + CHUNK_SIZE]
        batch_num = i // CHUNK_SIZE + 1
        logger.info(f"[lyrics] Mẻ {batch_num}/{total_batches} ({len(chunk_ids)} IDs)...")

        batch_rows = fetch_lyrics_batch(client, chunk_ids, batch_num, logger)
        append_to_csv(pd.DataFrame(batch_rows), output_file)

        has_lyrics = sum(1 for r in batch_rows if r["Lyrics"] != "No Lyrics")
        if has_lyrics > 0:
            success_count += 1
        else:
            fail_count += 1

        time.sleep(SLEEP_OK)

    logger.info(f"[PHA A] Xong: {success_count} mẻ có lyrics | {fail_count} mẻ 'No Lyrics'.")
    logger.info(f"[PHA A] File lưu tại: {output_file}")


# PHA B — LỌC TIẾNG VIỆT + LIÊN KHÚC
def detect_language(text):
    try:
        sample = str(text)[:300].strip()
        if not sample or sample == "♪":
            return "unknown"
        return detect(sample)
    except Exception:
        return "unknown"


def phase_b_filter_vietnamese(
    lyrics_raw_file, cleaned_data_file,
    output_vi_file, log_foreign_file, log_lienkhuc_file,
    logger
):
    if not os.path.exists(lyrics_raw_file):
        raise FileNotFoundError(f"Không tìm thấy '{lyrics_raw_file}'.")

    logger.info("PHA B - LỌC TIẾNG VIỆT + LIÊN KHÚC")

    # Đọc lyrics thô
    df = pd.read_csv(lyrics_raw_file, low_memory=False)
    logger.info(f"[-] Tổng bản ghi lyrics: {len(df):,}")

    # Join tên bài từ file Step 3 để lọc liên khúc theo tên
    track_name_col = None
    if os.path.exists(cleaned_data_file):
        try:
            df_meta = pd.read_csv(
                cleaned_data_file,
                low_memory=False,
                usecols=["Spotify_ID", "Track_Name"],
            )
            df_meta["Spotify_ID"] = df_meta["Spotify_ID"].astype(str).str.strip()
            df["Spotify_ID"] = df["Spotify_ID"].astype(str).str.strip()
            df = df.merge(df_meta, on="Spotify_ID", how="left")
            track_name_col = "Track_Name"
            logger.info(f"[join] Đã ghép tên bài từ '{cleaned_data_file}'.")
        except Exception as e:
            logger.warning(f"[join] Không ghép được tên bài: {e}. Chỉ lọc theo lyrics.")

    # Chuẩn hoá
    df["Lyrics"] = df["Lyrics"].fillna("No Lyrics")

    # Tầng 1: Lọc liên khúc
    logger.info("[lienkhuc] Đang phát hiện liên khúc / mashup / nonstop...")

    name_series = df[track_name_col] if track_name_col else pd.Series([""] * len(df))
    mask_lienkhuc = [
        is_lienkhuc(name, lyrics)
        for name, lyrics in zip(name_series, df["Lyrics"])
    ]
    df_lienkhuc = df[mask_lienkhuc].copy()
    df = df[~pd.Series(mask_lienkhuc, index=df.index)].copy()

    logger.info(
        f"[lienkhuc] Phát hiện và loại bỏ {len(df_lienkhuc):,} bài liên khúc. "
        f"Còn lại: {len(df):,} bài."
    )

    # Tầng 2: Lọc tiếng Việt
    mask_has_lyrics = (
        df["Lyrics"].astype(str).str.strip().ne("No Lyrics")
        & df["Lyrics"].astype(str).str.strip().ne("")
    )
    df_has_lyrics = df[mask_has_lyrics].copy()
    df_no_lyrics = df[~mask_has_lyrics].copy()

    logger.info(
        f"[pre-filter] Có lyrics: {len(df_has_lyrics):,} | "
        f"Không lyrics: {len(df_no_lyrics):,}."
    )

    logger.info(f"[langdetect] Đang nhận diện ngôn ngữ cho {len(df_has_lyrics):,} bài...")
    df_has_lyrics["Language"] = df_has_lyrics["Lyrics"].apply(detect_language)

    df_vi = df_has_lyrics[df_has_lyrics["Language"] == "vi"].copy()
    df_foreign = df_has_lyrics[df_has_lyrics["Language"] != "vi"].copy()

    # Xoá cột tạm
    cols_to_drop = [c for c in ["Language", "Track_Name"] if c in df_vi.columns]
    df_vi = df_vi.drop(columns=cols_to_drop)

    # Lưu kết quả
    safe_makedirs(output_vi_file)
    df_vi.to_csv(output_vi_file, index=False, encoding="utf-8-sig")
    logger.info(f"[PHA B] Thuần Việt : {len(df_vi):,} bài -> '{output_vi_file}'")

    # Log bài bị loại do ngoại ngữ / không lyrics
    safe_makedirs(log_foreign_file)
    df_dropped = pd.concat([df_foreign, df_no_lyrics], ignore_index=True)
    if not df_dropped.empty:
        keep_cols = [c for c in ["Spotify_ID", "Lyrics", "Language"] if c in df_dropped.columns]
        df_dropped[keep_cols].to_csv(log_foreign_file, index=False, encoding="utf-8-sig")
        logger.info(
            f"[PHA B] Ngoại ngữ/không lyrics: "
            f"{len(df_foreign):,} + {len(df_no_lyrics):,} -> '{log_foreign_file}'"
        )

    # Log bài bị loại do liên khúc
    safe_makedirs(log_lienkhuc_file)
    if not df_lienkhuc.empty:
        keep_cols = [c for c in ["Spotify_ID", "Track_Name", "Lyrics"] if c in df_lienkhuc.columns]
        df_lienkhuc[keep_cols].to_csv(log_lienkhuc_file, index=False, encoding="utf-8-sig")
        logger.info(
            f"[PHA B] Liên khúc / Mashup: "
            f"{len(df_lienkhuc):,} bài -> '{log_lienkhuc_file}'"
        )

    return len(df_vi)

# ENTRY POINT
def main():
    logger = get_logger("Step04_LyricsAndFilterVI", "step4.log")
    client = Spotify81Client()

    input_file = str(PipelineConfig.CLEANED_DATA_FILE)
    lyrics_raw_file = str(PipelineConfig.LYRICS_RAW_FILE)
    output_vi_file = str(PipelineConfig.VIETNAMESE_ONLY_FILE)
    log_foreign_file = str(PipelineConfig.DATA_DIR / "step4_removed_foreign.csv")
    log_lienkhuc_file = str(PipelineConfig.DATA_DIR / "step4_removed_lienkhuc.csv")  # log mới

    logger.info("=" * 55)
    logger.info("BƯỚC 4: TẢI LYRICS & LỌC TIẾNG VIỆT + LIÊN KHÚC")
    logger.info("=" * 55)
    logger.info(f"[-] Đầu vào (Bước 3): {input_file}")
    logger.info(f"[-] Lyrics thô: {lyrics_raw_file}")
    logger.info(f"[-] Lyrics thuần Việt: {output_vi_file}")
    logger.info(f"[-] Log ngoại ngữ: {log_foreign_file}")
    logger.info(f"[-] Log liên khúc: {log_lienkhuc_file}")
    logger.info("=" * 55)

    all_ids = load_input_ids(input_file, logger)

    phase_a_fetch_lyrics(
        client=client,
        all_ids=all_ids,
        output_file=lyrics_raw_file,
        logger=logger,
    )

    final_count = phase_b_filter_vietnamese(
        lyrics_raw_file=lyrics_raw_file,
        cleaned_data_file=input_file,
        output_vi_file=output_vi_file,
        log_foreign_file=log_foreign_file,
        log_lienkhuc_file=log_lienkhuc_file,
        logger=logger,
    )

    logger.info("=" * 55)
    logger.info("[THÀNH CÔNG] HOÀN TẤT BƯỚC 4")
    logger.info(f"[-] Bài hát tiếng Việt (không liên khúc): {final_count:,}")
    logger.info(f"[-] File đầu ra: {output_vi_file}")
    logger.info("=" * 55)


if __name__ == "__main__":
    try:
        main()
    except FileNotFoundError as err:
        print(f"\n[!] Lỗi: {err}")
    except KeyboardInterrupt:
        print("\n\n[!] ÉP DỪNG (Ctrl+C). Dữ liệu đã được lưu an toàn.")
    except Exception as e:
        print(f"\n[!] Lỗi không mong muốn: {e}")
        raise