import os
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

# helpers - đọc/ghi
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

    # lọc bỏ chuỗi rỗng và "nan"
    ids = [tid for tid in ids if tid and tid.lower() != "nan"]
    ids = list(dict.fromkeys(ids)) # giữ thứ tự, xoá trùng

    logger.info(f"[load_ids] Đọc OK: {len(ids):,} IDs hợp lệ từ '{input_file}'.")
    return ids

def load_fetched_ids(output_file, logger):

    if not os.path.exists(output_file):
        return set()
    
    try:
        df = pd.read_csv(output_file, low_memory=False, usecols=["Spotify_ID"])
        fetched = set(df["Spotify_ID"].astype(str).str.strip().tolist())
        logger.info(f"[resume] Đã có {len(fetched):,} IDs trong '{output_file}'. Bỏ qua khi chạy lại.")
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

# helpers - xử lý response lyrics
def extract_single_lyric(item):

    # bóc tách lời
    track_id = item.get("id")
    clean_text = "No Lyrics"

    # báo lỗi API
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

# pha A tải lyrics theo mẻ
def fetch_lyrics_batch(client, chunk_ids, batch_num, logger):

    querystring = {
        "ids": ",".join(chunk_ids),
        "format": "json",
    }

    for attempt in range(1, MAX_RETRIES + 1):
        data = client.get(ENDPOINT_LYRICS, params=querystring)

        if data is None:
            logger.warning(
                f"[lyrics] Mẻ {batch_num} - lần {attempt}/{MAX_RETRIES}: "
                "client trả về None, thử lại sau 5 giây..."
            )
            time.sleep(SLEEP_NET)
            continue

        # chuẩn hoá
        if isinstance(data, dict):
            items_list = data.get("results", [])
        elif isinstance(data, list):
            items_list = data
        else:
            logger.warning(
                f"[lyrics] mẻ {batch_num}: "
                f"Kiểu response không xác định ({type(data)}). Bỏ qua mẻ này."
            )
            break

        batch_rows = []
        for item in items_list:
            if not item or not isinstance(item, dict):
                continue
            track_id, lyrics_text = extract_single_lyric(item)
            if track_id:
                batch_rows.append({
                    "Spotify_ID": str(track_id),
                    "Lyrics": lyrics_text,
                })

        # ghi nhận những id bị api bỏ sót
        found_ids = {row["Spotify_ID"] for row in batch_rows}
        for tid in chunk_ids:
            if str(tid) not in found_ids:
                batch_rows.append({
                    "Spotify_ID": str(tid),
                    "Lyrics": "No Lyrics",
                })
        
        logger.info(
            f"[lyrics] Mẻ {batch_num}: "
            f"[THÀNH CÔNG] {len(batch_rows)}/{len(chunk_ids)} bài đã có kết quả."
        )
        time.sleep(SLEEP_OK)
        return batch_rows
    
    logger.error(
        f"[lyrics] Mẻ {batch_num}: "
        f"[LỖI] Thất bại sau {MAX_RETRIES} lần. Ghi 'No Lyrics' để tiếp tục pipeline."
    )
    return [{"Spotify_ID": str(tid), "Lyrics": "No Lyrics"} for tid in chunk_ids]

def phase_a_fetch_lyrics(client, all_ids, output_file, logger):

    fetched_ids = load_fetched_ids(output_file, logger)
    ids_to_fetch = [tid for tid in all_ids if tid not in fetched_ids]

    logger.info("PHA A - TẢI LYRICS")
    logger.info(f"[-] Tổng IDs : {len(all_ids):,}")
    logger.info(f"[-] Đã có (resume): {len(fetched_ids):,}")
    logger.info(f"[-] Cần tải thêm: {len(ids_to_fetch):,}")
    logger.info(f"[-] Kích thước mẻ: {CHUNK_SIZE}")

    if not ids_to_fetch:
        logger.info("[PHA A] Đã hoàn tất trước đó. Bỏ qua.")
        return
    
    safe_makedirs(output_file)

    total_batches = (len(ids_to_fetch) + CHUNK_SIZE - 1) // CHUNK_SIZE
    success_count = 0
    fail_count = 0

    for i in range(0, len(ids_to_fetch), CHUNK_SIZE):
        chunk_ids = ids_to_fetch[i : i + CHUNK_SIZE]
        batch_num = i // CHUNK_SIZE + 1

        logger.info(
            f"[lyrics] Mẻ {batch_num}/{total_batches} "
            f"({len(chunk_ids)} IDs)..."
        )

        batch_rows = fetch_lyrics_batch(client, chunk_ids, batch_num, logger)

        df_batch = pd.DataFrame(batch_rows)
        append_to_csv(df_batch, output_file)

        # kiểm tra có bài nào thực sự có lyrics không
        has_lyrics_count = sum(
            1 for row in batch_rows if row["Lyrics"] != "No Lyrics"
        )
        if has_lyrics_count > 0:
            success_count += 1
        else:
            fail_count += 1

        time.sleep(SLEEP_OK)
    
    logger.info(
        f"[PHA A] Hoàn tất: {success_count} mẻ có lyrics | "
        f"{fail_count} mẻ 'No Lyrics'."
    )
    logger.info(f"[PHA A] File lưu tại: {output_file}")

# pha B - Lọc tiếng việt bằng langetect
def detect_language(text):

    try:
        sample = str(text)[:300].strip()
        if not sample or sample == "♪":
            return "unknown"
        return detect(sample)
    except Exception:
        return "unknown"

def phase_b_filter_vietnamese(lyrics_raw_file, output_vi_file, log_foreign_file, logger):

    if not os.path.exists(lyrics_raw_file):
        raise FileNotFoundError(
            f"Không tìm thấy '{lyrics_raw_file}'. "
            f"Hãy đảm bảo Pha A đã chạy xong."
        )
    
    logger.info("PHA B - LỌC TIẾNG VIỆT (langdetect)")
    logger.info(f"[-] Đọc file: {lyrics_raw_file}")

    df = pd.read_csv(lyrics_raw_file, low_memory=False)
    initial_count = len(df)
    logger.info(f"[-] Tổng bản ghi đọc được: {initial_count:,}")

    if "Lyrics" not in df.columns:
        raise ValueError(f"File '{lyrics_raw_file}' Không có cột 'Lyrics'.")
    
    # Xoá trùng Spotify_ID
    before_dedup = len(df)
    df = df.drop_duplicates(subset=["Spotify_ID"], keep="first")
    if before_dedup != len(df):
        logger.info(
            f"[dedup] Đã xoá {before_dedup - len(df):,} dòng trùng Spotify_ID."
        )

    # lắp đầy nan ở cột Lyrics
    df["Lyrics"] = df["Lyrics"].fillna("No Lyrics")

    # loại bỏ những bài không có lyrics
    mask_has_lyrics = (
        df["Lyrics"].astype(str).str.strip().ne("No Lyrics")
        & df["Lyrics"].astype(str).str.strip().ne("")
    )
    df_has_lyrics = df[mask_has_lyrics].copy()
    df_no_lyrics = df[~mask_has_lyrics].copy()

    logger.info(
        f"[pre-filter] Có lyrics: {len(df_has_lyrics):,} | "
        f"Không lyrics: {len(df_no_lyrics):,} (bỏ qua nhận diện ngôn ngữ)."
    )

    # chạy langdetect trên những bài có lyrics
    logger.info(
        f"[langdetect] Đang nhận diện ngôn ngữ cho {len(df_has_lyrics):,} bài..."
    )
    df_has_lyrics["Language"] = df_has_lyrics["Lyrics"].apply(detect_language)

    # phân loại
    df_vi = df_has_lyrics[df_has_lyrics["Language"] == "vi"].copy()
    df_foreign = df_has_lyrics[df_has_lyrics["Language"] != "vi"].copy()

    # xoá cột Language tạm thời
    df_vi = df_vi.drop(columns=["Language"])
    final_vi_count = len(df_vi)
    dropped_foreign = len(df_foreign)
    dropped_no_lyrics = len(df_no_lyrics)

    # lưu file tiếng việt
    safe_makedirs(output_vi_file)
    df_vi.to_csv(output_vi_file, index=False, encoding="utf-8-sig")
    logger.info(
        f"[PHA B] Thuần Việt: {final_vi_count:,} bài -> '{output_vi_file}'"
    )

    # lưu log bài bị loại
    safe_makedirs(log_foreign_file)
    df_dropped = pd.concat([df_foreign, df_no_lyrics], ignore_index=True)

    if not df_dropped.empty:
        cols_to_keep = [
            col for col in ["Spotify_ID", "Lyrics", "Language"]
            if col in df_dropped.columns
        ]
        df_dropped[cols_to_keep].to_csv(log_foreign_file, index=False, encoding="utf-8-sig")
        logger.info(
            f"[PHA B] Đã xoá: {dropped_foreign:,} ngoại ngữ + "
            f"{dropped_no_lyrics:,} không lyrics -> '{log_foreign_file}'"
        )

    return final_vi_count

# entry point
def main():
    logger = get_logger("Step04_LyricsAndFilterVI", "step4.log")
    client = Spotify81Client()

    input_file = str(PipelineConfig.CLEANED_DATA_FILE)
    lyrics_raw_file = str(PipelineConfig.LYRICS_RAW_FILE)
    output_vi_file = str(PipelineConfig.VIETNAMESE_ONLY_FILE)
    log_foreign_file = str(PipelineConfig.DATA_DIR / "step4_removed_foreign.csv")

    logger.info("=" * 55)
    logger.info("BƯỚC 4: TẢI LYRICS & LỌC TIẾNG VIỆT")
    logger.info("=" * 55)
    logger.info(f"[-] Đầu vào (Bước 3) : {input_file}")
    logger.info(f"[-] Lyrics thô: {lyrics_raw_file}")
    logger.info(f"[-] Lyrics thuần Việt: {output_vi_file}")
    logger.info(f"[-] Log bài bị loại: {log_foreign_file}")
    logger.info("=" * 55)

    # nạp IDs
    all_ids = load_input_ids(input_file, logger)

    # pha A: tải lyrics theo mẻ, có chơ chế resume
    phase_a_fetch_lyrics(
        client = client,
        all_ids= all_ids,
        output_file= lyrics_raw_file,
        logger= logger,
    )

    # pha B: nhận diện ngôn ngữ, lọc giữ lại bài tiếng Việt
    final_count = phase_b_filter_vietnamese(
        lyrics_raw_file= lyrics_raw_file,
        output_vi_file= output_vi_file,
        log_foreign_file= log_foreign_file,
        logger= logger,
    )
    logger.info("=" * 55)
    logger.info("[THÀNH CÔNG] HOÀN TẤT BƯỚC 4")
    logger.info(f"[-] Bài hát tiếng Việt có lyrics: {final_count:,}")
    logger.info(f"[-] File đầu ra: {output_vi_file}")
    logger.info("=" * 55)

if __name__ == "__main__":
    try:
        main()
    except FileNotFoundError as err:
        print(f"\n[!] Lỗi: {err}")
    except KeyboardInterrupt:
        print(
            "\n\n[!] ÉP DỪNG CHƯƠNG TRÌNH (Ctrl+C). "
            "Dữ liệu đã được lưu an toàn - chạy lại để tiếp tục."
        )
    except Exception as e:
        print(f"\n[!] Lỗi không mong muốn: {e}")
        raise