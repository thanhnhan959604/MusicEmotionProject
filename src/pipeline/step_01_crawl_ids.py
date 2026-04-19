import time
import pandas as pd

from src.utils.config import PipelineConfig
from src.utils.logger import get_logger
from src.utils.http_client import Spotify81Client

SEED_KEYWORDS = PipelineConfig.SEED_KEYWORDS


# HELPERS - ĐỌC / GHI CACHE

def load_cache(logger):
    unique_track_ids = set()
    crawled_keywords = set()

    # Phục hồi IDs từ CSV đầu ra
    output_csv = str(PipelineConfig.CRAWLED_TRACKS_CSV)
    if PipelineConfig.CRAWLED_TRACKS_CSV.exists():
        try:
            df_existing = pd.read_csv(output_csv, usecols=["Spotify_ID"])
            unique_track_ids = set(df_existing["Spotify_ID"].astype(str).tolist())
            logger.info(f"[resume] CSV: Đã có {len(unique_track_ids):,} bài hát.")
        except Exception as e:
            logger.warning(f"[resume] Không đọc được CSV cũ: {e}. Bắt đầu lại từ đầu.")

    # Phục hồi keywords đã quét từ TXT
    if PipelineConfig.CRAWLED_ARTISTS_FILE.exists():
        with open(PipelineConfig.CRAWLED_ARTISTS_FILE, "r", encoding="utf-8") as f:
            crawled_keywords = {line.strip() for line in f if line.strip()}
        logger.info(f"[resume] Cache: Đã quét xong {len(crawled_keywords)} từ khóa.")

    return unique_track_ids, crawled_keywords


def save_keyword_cache(keyword):
    with open(PipelineConfig.CRAWLED_ARTISTS_FILE, "a", encoding="utf-8") as f:
        f.write(f"{keyword}\n")


def save_batch(batch_data, unique_track_ids, logger):
    output_csv = str(PipelineConfig.CRAWLED_TRACKS_CSV)

    # Ghi CSV (append)
    df_batch = pd.DataFrame(batch_data)
    write_header = not PipelineConfig.CRAWLED_TRACKS_CSV.exists()
    df_batch.to_csv(output_csv, mode="a", index=False, header=write_header, encoding="utf-8-sig")

    # Ghi track_ids.txt để Step 2 đọc (append)
    with open(PipelineConfig.TRACK_IDS_FILE, "a", encoding="utf-8") as f:
        for row in batch_data:
            f.write(f"{row['Spotify_ID']}\n")

    logger.info(
        f"   => Đã lưu {len(batch_data)} bài mới. "
        f"Tổng: {len(unique_track_ids):,}/{PipelineConfig.TARGET_COUNT:,}"
    )


# LOGIC CHÍNH

def main():
    logger = get_logger("Step01_CrawlIDs", "step1.log")
    client = Spotify81Client()

    logger.info("=" * 55)
    logger.info("BẮT ĐẦU: BƯỚC 1 - THU THẬP TRACK IDs")
    logger.info("=" * 55)
    logger.info(f"[-] Mục tiêu : {PipelineConfig.TARGET_COUNT:,} bài hát")
    logger.info(f"[-] Seed words : {len(SEED_KEYWORDS)} từ khóa")
    logger.info("-" * 55)

    unique_track_ids, crawled_keywords = load_cache(logger)

    for artist in SEED_KEYWORDS:
        if len(unique_track_ids) >= PipelineConfig.TARGET_COUNT:
            break

        if artist in crawled_keywords:
            continue

        logger.info(f"Đang tìm: '{artist}'")
        api_failed = False

        # 3 trang × 50 bài = tối đa 150 bài / keyword
        for offset in [0, 50, 100]:
            if len(unique_track_ids) >= PipelineConfig.TARGET_COUNT:
                break

            params = {
                "q": artist,
                "type": "tracks",
                "limit": "50",
                "offset": str(offset),
            }

            search_data = client.get("/search", params=params)

            if not search_data:
                api_failed = True
                logger.warning(
                    f"[!] API thất bại khi đang quét '{artist}' (offset={offset}). "
                    "Dừng để tránh mất dữ liệu."
                )
                break

            # Bóc tách JSON — tương thích 2 cấu trúc response của spotify81
            items = []
            if "tracks" in search_data and "items" in search_data["tracks"]:
                items = search_data["tracks"]["items"]
            elif "tracks" in search_data:
                items = search_data["tracks"]

            if not items:
                break  # Hết bài ở trang này

            batch_data = []
            for track in items:
                track_info = track.get("data", track)

                track_id = track_info.get("id")
                if not track_id or track_id in unique_track_ids:
                    continue

                track_name = track_info.get("name", "Unknown")

                # Bóc tách nghệ sĩ
                artists_list = track_info.get("artists", {}).get("items", [])
                if not artists_list:
                    artists_list = track_info.get("artists", [])

                artist_names = []
                for a in artists_list:
                    name = a.get("profile", {}).get("name") or a.get("name")
                    if name:
                        artist_names.append(name)

                artist_str = ", ".join(artist_names) if artist_names else "Unknown"

                unique_track_ids.add(track_id)
                batch_data.append({
                    "Spotify_ID": track_id,
                    "Track_Name": track_name,
                    "Artist": artist_str,
                })

                if len(unique_track_ids) >= PipelineConfig.TARGET_COUNT:
                    break

            if batch_data:
                save_batch(batch_data, unique_track_ids, logger)

            time.sleep(1.5)  # Tránh rate limit

        # Dừng toàn bộ nếu API chết giữa chừng
        if api_failed:
            logger.error(
                f"[!] Dừng khẩn cấp tại keyword '{artist}'. "
                "Hãy cập nhật RAPIDAPI_KEY mới rồi chạy lại — code sẽ tiếp tục từ đây."
            )
            break

        # Lưu keyword vào cache sau khi quét thành công cả 3 trang
        save_keyword_cache(artist)
        crawled_keywords.add(artist)

    logger.info("=" * 55)
    if len(unique_track_ids) >= PipelineConfig.TARGET_COUNT:
        logger.info(f"[HOÀN TẤT] Đã đạt mục tiêu {PipelineConfig.TARGET_COUNT:,} bài hát.")
    else:
        logger.warning(
            f"[KẾT THÚC] Thu được {len(unique_track_ids):,} IDs. "
            "Thêm keyword vào SEED_KEYWORDS nếu muốn nhiều hơn."
        )
    logger.info(f"[-] CSV : {PipelineConfig.CRAWLED_TRACKS_CSV}")
    logger.info(f"[-] IDs file : {PipelineConfig.TRACK_IDS_FILE}")
    logger.info("=" * 55)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n\n[!] ÉP DỪNG (Ctrl+C). Dữ liệu đã được lưu an toàn.")
    except Exception as e:
        print(f"\n[!] Lỗi không mong muốn: {e}")
        raise