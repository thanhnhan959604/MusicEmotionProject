import os
import concurrent.futures
import requests
import pandas as pd
from src.utils.config import PipelineConfig
from src.utils.logger import get_logger

# Hằng số
MAX_WORKERS = 10    # số luồng tải song song
DOWNLOAD_TIMEOUT = 20    # timeout mỗi request tải file (giây)
CHUNK_SIZE = 8192  # kích thước chunk đọc stream (bytes)

# trạng thái kết quả từng bài
STATUS_SUCCESS = "[THÀNH CÔNG]"
STATUS_SKIP = "[BỎ QUA ĐÃ TẢI]"
STATUS_HTTP_ERR = "[LỖI HTTP]"
STATUS_NET_ERR = "[LỖI MẠNG]"

# helpers - I/O
def load_master(filepath, logger):
    # đọc master dataset từ bước 5.
    if not os.path.exists(filepath):
        raise FileNotFoundError(
            f"Không tìm thấy '{filepath}'. "
            "Hãy chạy step_05_merge_master_dataset.py trước."
        )
    try:
        df = pd.read_csv(filepath, low_memory=False)
        logger.info(f"[LOAD] Đọc OK: {len(df):,} bài hát từ '{filepath}'.")
        return df
    except Exception as err:
        logger.error(f"[LOAD] Không đọc được file: {err}")
        raise


def save_log(rows, log_file, logger):
    # ghi log kết quả tải vào CSV, append nếu file đã tồn tại.
    if not rows:
        return

    df_log = pd.DataFrame(rows)
    write_header = not os.path.exists(log_file)

    df_log.to_csv(
        log_file,
        mode="a",
        index=False,
        header=write_header,
        encoding="utf-8-sig",
    )
    logger.info(f"[LOG] Đã ghi {len(rows):,} dòng vào '{log_file}'.")


def safe_makedirs(filepath):
    # tạo thư mục cha nếu chưa tồn tại.
    dirpath = os.path.dirname(filepath)
    if dirpath:
        os.makedirs(dirpath, exist_ok=True)

# helpers - chuẩn bị dữ liệu

def load_downloaded_ids(audio_dir):
    # quét thư mục audio để lấy danh sách ID đã tải.
    if not os.path.exists(audio_dir):
        return set()

    downloaded = set()
    for filename in os.listdir(audio_dir):
        if filename.endswith(".mp3"):
            track_id = filename.replace(".mp3", "").strip()
            downloaded.add(track_id)
    return downloaded


def extract_valid_url(row):
    # lấy Preview_Audio_URL hợp lệ từ một dòng dữ liệu.
    url_val = row.get("Preview_Audio_URL", "")

    if pd.isna(url_val):
        return ""

    url_str = str(url_val).strip()

    if url_str.startswith("http"):
        return url_str

    return ""


def build_task_list(df, downloaded_ids, logger):
    # xây dựng danh sách tác vụ cần tải, bỏ qua bài đã có file.
    tasks = []
    skip_count = 0
    no_url_count = 0

    for row in df.to_dict("records"):
        track_id = str(row.get("Spotify_ID", "")).strip()
        if not track_id or track_id.lower() == "nan":
            continue

        # bỏ qua nếu đã tải rồi
        if track_id in downloaded_ids:
            skip_count += 1
            continue

        url = extract_valid_url(row)

        # bỏ qua nếu không có URL preview
        if not url:
            no_url_count += 1
            continue

        track_name = str(row.get("Track_Name", "Unknown"))
        tasks.append({
            "track_id" : track_id,
            "track_name" : track_name,
            "url" : url,
        })

    logger.info(
        f"[BUILD TASKS] Tổng bài: {len(df):,} | "
        f"Đã tải sẵn: {skip_count:,} | "
        f"Không có URL: {no_url_count:,} | "
        f"Cần tải: {len(tasks):,}."
    )

    # trả về cả no_url_count để log_summary dùng lại đúng số liệu
    return tasks, no_url_count

# logic tải file

def download_one(task, audio_dir):
    # tải 1 file preview MP3, trả về dict kết quả.
    track_id = task["track_id"]
    track_name = task["track_name"]
    url = task["url"]
    save_path = os.path.join(audio_dir, f"{track_id}.mp3")

    # xử lý trường hợp file xuất hiện trong lúc chờ luồng khác
    if os.path.exists(save_path):
        return {
            "Spotify_ID" : track_id,
            "Track_Name" : track_name,
            "Status" : STATUS_SKIP,
            "File_Path" : save_path,
        }

    try:
        response = requests.get(url, stream=True, timeout=DOWNLOAD_TIMEOUT)

        if response.status_code == 200:
            with open(save_path, "wb") as f:
                for chunk in response.iter_content(chunk_size=CHUNK_SIZE):
                    if chunk:
                        f.write(chunk)

            return {
                "Spotify_ID" : track_id,
                "Track_Name" : track_name,
                "Status" : STATUS_SUCCESS,
                "File_Path" : save_path,
            }

        # ghi HTTP lỗi vào status.
        return {
            "Spotify_ID" : track_id,
            "Track_Name" : track_name,
            "Status" : f"{STATUS_HTTP_ERR}_{response.status_code}",
            "File_Path" : "",
        }

    except requests.exceptions.Timeout:
        return {
            "Spotify_ID" : track_id,
            "Track_Name" : track_name,
            "Status" : f"{STATUS_NET_ERR}_timeout",
            "File_Path" : "",
        }

    except requests.exceptions.RequestException as err:
        return {
            "Spotify_ID" : track_id,
            "Track_Name" : track_name,
            "Status" : f"{STATUS_NET_ERR}_{str(err)[:40]}",
            "File_Path" : "",
        }

# Bước chính - tải đa luồng

def step_download_parallel(tasks, audio_dir, log_file, logger):
    # tải toàn bộ danh sách tác vụ bằng ThreadPoolExecutor.
    total = len(tasks)
    results = []
    success = 0
    fail = 0
    completed = 0

    logger.info(
        f"[DOWNLOAD] Bắt đầu tải {total:,} bài "
        f"với {MAX_WORKERS} luồng song song..."
    )

    with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:

        # nộp toàn bộ tác vụ vào thread pool
        future_map = {
            executor.submit(download_one, task, audio_dir): task
            for task in tasks
        }

        for future in concurrent.futures.as_completed(future_map):
            completed += 1
            result = future.result()
            results.append(result)

            if result["Status"] == STATUS_SUCCESS:
                success += 1
            else:
                fail += 1

            # log tiến trình mỗi 50 bài
            if completed % 50 == 0 or completed == total:
                logger.info(
                    f"[DOWNLOAD] Tiến trình: {completed}/{total} "
                    f"| Thành công: {success} | Lỗi: {fail}"
                )

    # ghi toàn bộ log ra CSV một lần sau khi tải xong để tránh race condition
    save_log(results, log_file, logger)

    logger.info(
        f"[DOWNLOAD] Hoàn tất: {success:,} thành công | {fail:,} thất bại."
    )
    return results

# kết quả trả về

def log_summary(results, no_url_count, audio_dir, log_file, logger):

    success  = sum(1 for r in results if r["Status"] == STATUS_SUCCESS)
    skipped  = sum(1 for r in results if r["Status"] == STATUS_SKIP)

    http_err = sum(1 for r in results if r["Status"].startswith(STATUS_HTTP_ERR))
    net_err  = sum(1 for r in results if r["Status"].startswith(STATUS_NET_ERR))

    # đếm tổng file MP3 thực tế sau khi tải
    total_mp3 = len([f for f in os.listdir(audio_dir) if f.endswith(".mp3")])

    logger.info("=" * 55)
    logger.info("[THANH CONG] HOAN TAT BUOC 6")
    logger.info(f"[-] Tải thành công: {success:,} bài")
    logger.info(f"[-] Bỏ qua (đã có): {skipped:,} bài")
    logger.info(f"[-] Không có URL: {no_url_count:,} bài")
    logger.info(f"[-] Lỗi HTTP: {http_err:,} bài")
    logger.info(f"[-] Lỗi mạng: {net_err:,} bài")
    logger.info(f"[-] Tổng MP3 trên đĩa: {total_mp3:,} file")
    logger.info(f"[-] Thư mục audio: {audio_dir}")
    logger.info(f"[-] Log chi tiết: {log_file}")
    logger.info("=" * 55)

# Entry point
def main():
    logger = get_logger("Step06_DownloadAudioPreview", "step6.log")

    master_file = str(PipelineConfig.MASTER_DATASET_FILE)  # Bước 5
    audio_dir = str(PipelineConfig.AUDIO_DIR)
    log_file = str(PipelineConfig.DOWNLOAD_LOG_FILE)

    logger.info("=" * 55)
    logger.info("BUOC 6: TAI AUDIO PREVIEW (DA LUONG)")
    logger.info("=" * 55)
    logger.info(f"[-] Master dataset (Buoc 5): {master_file}")
    logger.info(f"[-] Thu muc audio: {audio_dir}")
    logger.info(f"[-] Log ket qua: {log_file}")
    logger.info(f"[-] So luong song song: {MAX_WORKERS} luong")
    logger.info("-" * 55)

    # đảm bảo thư mục audio và thư mục chứa log tồn tại
    os.makedirs(audio_dir, exist_ok=True)
    safe_makedirs(log_file)

    # đọc master dataset
    df = load_master(master_file, logger)

    # quét các file đã tải để resume nếu bị ngắt giữa chừng
    downloaded_ids = load_downloaded_ids(audio_dir)
    logger.info(
        f"[resume] Tìm thấy {len(downloaded_ids):,} file MP3 sẵn có trong '{audio_dir}'."
    )

    # xây dựng danh sách tác vụ cần tải
    # no_url_count: số bài bị loại do không có URL, truyền vào log_summary
    tasks, no_url_count = build_task_list(df, downloaded_ids, logger)

    if not tasks:
        logger.info("[main] Tất cả audio đã được tải. Không cần tải thêm.")
        return

    # tải đa luồng và ghi log
    results = step_download_parallel(tasks, audio_dir, log_file, logger)

    # báo cáo tổng kết
    log_summary(results, no_url_count, audio_dir, log_file, logger)


if __name__ == "__main__":
    try:
        main()
    except FileNotFoundError as err:
        print(f"\n[!] Lỗi: {err}")
    except KeyboardInterrupt:
        print(
            "\n\n[!] ÉP DỪNG CHƯƠNG TRÌNH (Ctrl+C). "
            "Các file đã tải được giữ nguyên. Chạy lại để tiếp tục."
        )
    except Exception as err:
        print(f"\n[!] Lỗi không mong muốn: {err}")
        raise