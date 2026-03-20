import os
import time
from collections import deque

from src.utils.config import PipelineConfig
from src.utils.logger import get_logger
from src.utils.http_client import Spotify81Client

# đọc cache
def load_cache():

    track_ids = set()
    crawled_artists = set()

    if os.path.exists(PipelineConfig.TRACK_IDS_FILE):
        with open(PipelineConfig.TRACK_IDS_FILE, "r", encoding="utf-8") as f:
            track_ids = {line.strip() for line in f if line.strip()}
 
    if os.path.exists(PipelineConfig.CRAWLED_ARTISTS_FILE):
        with open(PipelineConfig.CRAWLED_ARTISTS_FILE, "r", encoding="utf-8") as f:
            crawled_artists = {line.strip() for line in f if line.strip()}

    return track_ids, crawled_artists

# ghi cache real-time
def save_cache(filepath, item):

    with open(filepath, "a", encoding="utf-8") as f:
        f.write(f"{item}\n")

# bóc tách nghệ sĩ từ track node
def extract_artists(track_data):

    artists_names = []
    artists_items = track_data.get("artists", {}).get("items", [])

    for artist_node in artists_items:
        # tên ca sĩ trong profile or bên ngoài
        name = (
            artist_node.get("profile", {}).get("name")
            or artist_node.get("name")
        )
        if name:
            artists_names.append(name)

    return artists_names

# main
def main():

    logger = get_logger("Step01_CrawlIDs", "step1.log")
    client = Spotify81Client()

    # lịch sử quét từ cache
    unique_track_ids, crawled_artists = load_cache()

    # xử lý các nghệ sĩ đã quét
    artist_queue = deque(
        keyword
        for keyword in PipelineConfig.SEED_KEYWORDS
        if keyword.lower() not in crawled_artists
    )

    logger.info("=" * 55)
    logger.info("BẮT ĐẦU: THẢ NHỆN GOM ID NHẠC VIỆT")
    logger.info("=" * 55)
    logger.info(f"[-] Mục tiêu: {PipelineConfig.TARGET_COUNT:,} IDs")
    logger.info(f"[-] Đang có trong kho: {len(unique_track_ids):,} IDs")
    logger.info(f"[-] Hàng đợi ban đầu: {len(artist_queue)} từ khoá")
    logger.info("-" * 55)

    # vòng lặp bfs (vết dầu loang)
    while artist_queue and len(unique_track_ids) < PipelineConfig.TARGET_COUNT:

        current_artist = artist_queue.popleft()
        current_artist_lower = current_artist.lower()

        if current_artist_lower in crawled_artists:
            continue

        logger.info(
            f"Đang đào: '{current_artist}' "
            f"(Tổng gom: {len(unique_track_ids):,} IDs | "
            f"Hàng đợi: {len(artist_queue)})"
        )

        # đánh dấu và lưu cache trước khi gọi API
        crawled_artists.add(current_artist_lower)
        save_cache(PipelineConfig.CRAWLED_ARTISTS_FILE, current_artist_lower)

        # gọi search API qua Spotify81Client
        params = {
            "q" : current_artist,
            "type" : "multi",
            "offset" : "0",
            "limit" : "50",
            "numberOfTopResults" : "5"
        }
        search_data = client.get("/search/", params=params)

        # bỏ qua API nếu không trả về dữ liệu hợp lệ
        if not search_data or "tracks" not in search_data:
            logger.warning(f"Không có kết quả cho '{current_artist}'. Bỏ qua.")
            time.sleep(2.5)
            continue

        # bóc tách JSON của Spotify
        new_ids_this_round = 0

        for track_node in search_data["tracks"]:
            track_data = track_node.get("data", {})
            track_id = track_data.get("id")

            if track_id and track_id not in unique_track_ids:
                # cắt track id vào kho và lưu cache
                unique_track_ids.add(track_id)
                save_cache(PipelineConfig.TRACK_IDS_FILE, track_id)
                new_ids_this_round += 1

                # bóc nghệ sĩ liên quan cho vào hàng đợi bfs
                for artist_name in extract_artists(track_data):
                    if artist_name.lower() not in crawled_artists:
                        artist_queue.append(artist_name)

            # ngắt sớm nếu đủ mục tiêu
            if len(unique_track_ids) >= PipelineConfig.TARGET_COUNT:
                break

        logger.info(
            f" -> Thêm {new_ids_this_round} ID mới. "
            f"Tổng cộng: {len(unique_track_ids):,}/{PipelineConfig.TARGET_COUNT:,}."
        )

        time.sleep(2.5) # khoảng nghỉ rate limit

    logger.info("=" * 55)

    if len(unique_track_ids) >= PipelineConfig.TARGET_COUNT:
        logger.info(f"HOÀN TẤT: đã đạt mục tiêu {PipelineConfig.TARGET_COUNT:,} IDs. ")
    else:
        logger.warning(
            f"Hàng đợi đã rỗng. Chỉ gom được {len(unique_track_ids):,} IDs. "
            f"Hãy thêm `SEED_KEYWORDS` trong `config.py` để mở rộng."
        )

    logger.info(f"Kết quả cuối cùng: {len(unique_track_ids):,} Track IDs. ")
    logger.info(f"Cache lưu tại: {PipelineConfig.TRACK_IDS_FILE}")
    logger.info("=" * 55)

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print(
            "\n\n[!] ÉP DỪNG CHƯƠNG TRÌNH (Ctrl+C). "
            "Dữ liệu đã được lưu an toàn."
        )
    except Exception as e:
        print(f"\n[!] Lỗi: {e}")