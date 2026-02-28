import requests
import time
import os
import pandas as pd
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock
from tqdm import tqdm

JAMENDO_CLIENT_ID = "210b8559"
METADATA_FILE = "data/metadata/splits/audio_metadata_part_13.csv"
OUTPUT_FILE = "data/lyrics/splits_jamendo/jamendo_lyrics_13.csv"

MAX_WORKERS = 3
DELAY_SECONDS = 5

#gọi API một TRACK
def get_single_track(track_id):
    clean_id = str(track_id).replace("track_", "")
    url = "https://api.jamendo.com/v3.0/tracks/"

    params = {
        "client_id": JAMENDO_CLIENT_ID,
        "id": clean_id,
        "format": "json",
        "include": "lyrics",
        "limit": 1
    }

    try:
        response = requests.get(url=url, params=params, timeout=10)

        if response.status_code != 200:
            print("[ERROR] HTTP: ", response.status_code)
            return None
        

        data = response.json()

        if "results" not in data:
            return None
        
        if len(data["results"]) == 0:
            return None
        
        return data["results"][0]
    
    except Exception as e:
        print("[ERROR] ", e)
        return None

#THREAD xử lý 1 TRẠCk
def process_track(track_id):
    print("Xử lý track: ", track_id)

    track_info = get_single_track(track_id=track_id)

    lyric_text = ""

    if track_info is not None:
        lyrics = track_info.get("lyrics")
        if lyrics is not None:
            if str(lyrics).strip() != "":
                lyric_text = str(lyrics).strip()
    
    time.sleep(DELAY_SECONDS)

    return {
        "TRACK_ID": track_id,
        "LYRIC": lyric_text
    }
   
#lấy lyric từ jamendo
def fetch_lyrics():
    
    if not os.path.exists(METADATA_FILE):
        print("[ERROR] Không tìm thấy file metadata")
        return
    
    df = pd.read_csv(METADATA_FILE, encoding="utf-8-sig")

    track_id_list = df["TRACK_ID"].astype(str).to_list()

    total_tracks = len(track_id_list)

    print("Tổng bài: ", total_tracks)
    print("Số luồng: ", MAX_WORKERS)
    
    final_results = []
    count_has_lyrics = 0
    count_no_lyrics = 0

    os.makedirs("data/lyrics/splits_jamendo", exist_ok=True)

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:

        futures = []

        for track_id in track_id_list:
            future = executor.submit(process_track, track_id)
            futures.append(future)
        for future in tqdm(as_completed(futures), total=total_tracks, desc="Processing"):
            result = future.result()
            final_results.append(result)

            if result.get("LYRIC") and result.get("LYRIC").strip() != "":
                count_has_lyrics += 1
            else:
                count_no_lyrics += 1

    print("\n[THỐNG KÊ]: ")
    print("Số bài có lời: ", count_has_lyrics)
    print("Số bài không có lời: ", count_no_lyrics)

    #lưu file

    df_save = pd.DataFrame(final_results)

    df_save.to_csv(OUTPUT_FILE, index=False, encoding="utf-8-sig")

    print("\nĐã lưu file", OUTPUT_FILE)

if __name__ == "__main__":
    fetch_lyrics()
        




