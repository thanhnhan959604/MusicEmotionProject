import os
import time
import pandas as pd

from src.utils.config import PipelineConfig
from src.utils.logger import get_logger
from src.utils.http_client import Spotify81Client

# hằng số
CHUNK_SIZE = 50
SLEEP_OK = 2.0
SLEEP_RETRY = 5.0
MAX_RETRIES = 3

# 13 đặc trưng 
AUDIO_FEATURE_COLS = [
    "danceability", "energy", "key", "loudness", "mode",
    "speechiness", "acousticness", "instrumentalness",
    "liveness", "valence", "tempo", "duration_ms", "time_signature",
]

# helper chung
def load_track_ids():
    csv_file = PipelineConfig.CRAWLED_TRACKS_CSV
    
    if not os.path.exists(csv_file):
        raise FileNotFoundError(
            f"Không tìm thấy '{csv_file}'. "
            "Hãy chạy `step01_crawl_ids.py` trước."
        )
    
    try:
        # Dùng pandas đọc file CSV, chỉ load đúng cột 'Spotify_ID' để tiết kiệm RAM
        df = pd.read_csv(csv_file, usecols=["Spotify_ID"])
        
        # Chuyển thành dạng chuỗi, xóa khoảng trắng thừa và đưa vào một set (tập hợp không trùng lặp)
        track_ids = set(df["Spotify_ID"].astype(str).str.strip().tolist())
        
        return track_ids
        
    except Exception as e:
        raise RuntimeError(f"Lỗi khi đọc file CSV '{csv_file}': {e}")
    
def load_fetched_ids(output_file, id_col):

    if not os.path.exists(output_file):
        return set()
    
    try:
        # đọc id
        df = pd.read_csv(output_file, low_memory=False, usecols=[id_col])
        return set(df[id_col].astype(str).str.strip().tolist())
    except Exception:
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

# pha A - lấy metadata thô

def fetch_and_save_batch(client, endpoint, key_name, chunk_ids, output_file, batch_num, logger):
    
    querystring = {"ids": ",".join(chunk_ids)}
    for attempt in range(1, MAX_RETRIES + 1):
        # gọi api Spotify81Client, tự xử lý 429 + lỗi mạng
        data = client.get(endpoint, params=querystring)

        # hết retry or 403
        if data is None:
            logger.warning(
                f"[{endpoint}] Mẻ {batch_num} - lần {attempt}/{MAX_RETRIES}: "
                "client trả về None, thử lại.."
            )
            time.sleep(SLEEP_RETRY)
            continue

        # chuẩn hoá
        if isinstance(data, list):
            items_list = data
        elif isinstance(data, dict):
            items_list = data.get(key_name, [])
        else:
            logger.warning(
                f"[{endpoint}] Mẻ {batch_num}: "
                f"Kiểu response không xác định ({type(data)}). Bỏ qua."
            )
            return False
        
        # không lưu trống - null
        if not items_list:
            logger.warning(f"[{endpoint}] Mẻ {batch_num}: Dữ liệu trống.")
            return False
        
        # lọc items null
        valid_items = [item for item in items_list if item and isinstance(item, dict)]

        if not valid_items:
            logger.warning(f"[{endpoint}] Mẻ {batch_num}: Không có item hợp lệ.")
            return False
        
        df_batch = pd.json_normalize(valid_items)

        append_to_csv(df_batch, output_file)

        logger.info(
            f"[{endpoint}] Mẻ {batch_num}: "
            f"[THÀNH CÔNG] Lưu {len(df_batch)}/{len(chunk_ids)} records "
            f"-> {os.path.basename(output_file)}"
        )
        time.sleep(SLEEP_OK)
        return True
    
    logger.error(f"[{endpoint}] Mẻ {batch_num}: [LỖI] Thất bại sau {MAX_RETRIES} lần.")
    return False

# pha B - lấy audio features
def fetch_and_save_audio_features_batch(client, chunk_ids, output_file, batch_num, logger):

    querystring = {"ids": ",".join(chunk_ids)}

    for attempt in range(1, MAX_RETRIES + 1):
        data = client.get("/audio_features", params=querystring)

        if data is None:
            logger.warning(
                f"[/audio_features] Mẻ {batch_num} - lần {attempt}/{MAX_RETRIES}: "
                "client trả về None, thử lại.."
            )
            time.sleep(SLEEP_RETRY)
            continue

        items_list = data.get("audio_features", []) if isinstance(data, dict) else data

        # tra cứu rỗng
        rows = {}

        for item in items_list:
            if not item or not isinstance(item, dict):
                continue

            track_id = item.get("id")
            if not track_id:
                continue

            row = {"Spotify_ID": str(track_id)}
            for feat in AUDIO_FEATURE_COLS:
                row[feat] = item.get(feat, "")

            rows[str(track_id)] = row

        for tid in chunk_ids:
            if str(tid) not in rows:
                empty_row = {"Spotify_ID": str(tid)}
                for feat in AUDIO_FEATURE_COLS:
                    empty_row[feat] = ""
                rows[str(tid)] = empty_row
            
        df_batch = pd.DataFrame(list(rows.values()))

        append_to_csv(df_batch, output_file)

        # số bài có features
        valid_count = sum(1 for row in rows.values() if row.get("danceability") != "")
        null_count = len(chunk_ids) - valid_count

        logger.info(
            f"[/audio_features] Mẻ {batch_num}: "
            f"[THÀNH CÔNG] {valid_count}/{len(chunk_ids)} có features "
            f"({null_count} null -> ghi dòng rỗng) "
            f"-> {os.path.basename(output_file)}"
        )
        time.sleep(SLEEP_OK)
        return True
    
    logger.error(
        f"[/audio_features] Mẻ {batch_num}: "
        f"[LỖI] Thất bại sau {MAX_RETRIES} lần. Ghi dòng rỗng để tiếp tục pipeline."
    )
    empty_rows = []
    for tid in chunk_ids:
        empty_row = {"Spotify_ID": str(tid)}
        for feat in AUDIO_FEATURE_COLS:
            empty_row[feat] = ""
        empty_rows.append(empty_row)

    append_to_csv(pd.DataFrame(empty_rows), output_file)
    return False

# điều phối chung
def process_endpoint(client, all_ids, endpoint, key_name, id_col, output_file, logger):

    fetched_ids = load_fetched_ids(output_file, id_col)

    ids_to_fetch = list(all_ids - fetched_ids)

    logger.info(f"ĐANG XỬ LÝ: {endpoint}...")
    logger.info(f"[-] Tổng IDs : {len(all_ids):,}")
    logger.info(f"[-] Đã có (resume) : {len(fetched_ids):,} <- bỏ qua khi chạy lại")
    logger.info(f"[-] Cần tải thêm : {len(ids_to_fetch):,}")
    logger.info(f"[-] Tự động lưu sau : mỗi {CHUNK_SIZE} bài")

    # end sớm
    if not ids_to_fetch:
        logger.info(f"[THÀNH CÔNG] đã hoàn tất {endpoint}. Bỏ qua.")
        return
    
    safe_makedirs(output_file)

    # tổng số Mẻ
    total_batches = (len(ids_to_fetch) + CHUNK_SIZE - 1) // CHUNK_SIZE
    success_count = 0
    fail_count = 0

    for i in range(0, len(ids_to_fetch), CHUNK_SIZE):

        chunk_ids = ids_to_fetch[i : i + CHUNK_SIZE]
        batch_num = i // CHUNK_SIZE + 1

        logger.info(f"[{endpoint}] -> Mẻ {batch_num}/{total_batches} ({len(chunk_ids)} IDs)...")

        if endpoint == "/audio_features":
            ok = fetch_and_save_audio_features_batch(
                client, chunk_ids, output_file, batch_num, logger
            )
        else:
            ok = fetch_and_save_batch(
                client, endpoint, key_name, chunk_ids, output_file, batch_num, logger
            )

        if ok:
            success_count += 1
        else:
            fail_count += 1

    logger.info(
        f"Hoàn tất {endpoint}: [THÀNH CÔNG] {success_count} mẻ | [LỖI] {fail_count} mẻ"
    )

# entry point
def main():
    
    logger = get_logger("Step02_FetchMetadataFeatures", "step2.log")
    client = Spotify81Client()

    meta_file = str(PipelineConfig.RAW_METADATA_FILE)
    audio_feat_file = str(PipelineConfig.AUDIO_FEATURES_FILE)

    logger.info("=" * 55)
    logger.info("BƯỚC 2: LẤY METADATA & AUDIO FEATURES TỪ SPOTIFY")
    logger.info("=" * 55)

    # nạp ids từ Step01
    all_ids = load_track_ids()
    logger.info(f"[-] Tổng Track IDs từ cache: {len(all_ids):,}")

    # pha A
    process_endpoint(
        client = client,
        all_ids = all_ids,
        endpoint = "/tracks",
        key_name = "tracks",
        id_col = "id",
        output_file = meta_file,
        logger = logger,
    )

    # pha B
    process_endpoint(
        client = client,
        all_ids = all_ids,
        endpoint = "/audio_features",
        key_name = "audio_features",
        id_col = "Spotify_ID",
        output_file = audio_feat_file,
        logger = logger,
    )

    logger.info("=" * 55)
    logger.info("[THÀNH CÔNG] HOÀN TẤT TOÀN BỘ BƯỚC 2")
    logger.info(f"Metadata -> {meta_file}")
    logger.info(f"Audio Features -> {audio_feat_file}")
    logger.info("=" * 55)

if __name__ == "__main__":
    try:
        main()
    except FileNotFoundError as e:
        print(f"\n[!] Lỗi: {e}")
    except KeyboardInterrupt:
        print(
            "\n\n[!] ÉP DỪNG CHƯƠNG TRÌNH (Ctrl+C). "
            "Dữ liệu đã được lưu an toàn."
        )
    except Exception as e:
        print(f"\n[!] Lỗi không mong muốn: {e}")