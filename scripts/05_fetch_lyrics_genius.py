import os
import csv
import time
import pandas as pd
import lyricsgenius
from tqdm import tqdm


# ===== CẤU HÌNH (CONFIG) =====
GENIUS_TOKEN = "_8c82WX9Uv7LkurXwTdiJSCWX1m0U8g_3Kv-BeBYI24T0etcTzsbZbkLH5EspZ7A"
METADATA_FILE = r"D:\KY_6\DA2\MusicEmotionProject\data\metadata\splits\audio_metadata_part_5.csv"
OUTPUT_FILE = "data/lyrics/genius_lyrics_5.csv"
SLEEP_TIME = 2


def load_metadata():
    if not os.path.exists(METADATA_FILE):
        print("Không tìm thấy metadata file")
        return None

    df = pd.read_csv(METADATA_FILE, encoding="utf-8-sig")
    df["TRACK_ID"] = df["TRACK_ID"].astype(str)
    return df

def create_client():
    genius = lyricsgenius.Genius(
        GENIUS_TOKEN,
        timeout=15,
        sleep_time=1,
        retries=3
    )
    genius.verbose = True
    genius.remove_section_headers = True
    genius.skip_non_songs = True
    genius.excluded_terms = ["(Remix)", "(Live)"]
    return genius

def extract_year_from_song(song):
    """
    Lấy năm từ release_date_components trong JSON gốc
    """
    try:
        if hasattr(song, "_body"):
            body = song._body
            if "release_date_components" in body:
                comp = body["release_date_components"]
                if comp and "year" in comp and comp["year"]:
                    return str(comp["year"])
    except Exception:
        return None
    return None

def fetch_lyrics():
    print("===== TẢI LỜI BÀI HÁT TỪ GENIUS =====")

    df_meta = load_metadata()
    if df_meta is None:
        return

    genius = create_client()

    rows = []
    success = 0
    fail = 0

    for _, row in tqdm(df_meta.iterrows(), total=len(df_meta), desc="Processing"):
        track_id = str(row["TRACK_ID"])
        title = str(row["TITLE"]).strip()
        artist = str(row["ARTIST"]).strip()
        metadata_year = str(row["YEAR"]).strip()

        print("\nĐang xử lý:", track_id)

        if not title or not artist or title == 'nan' or artist == 'nan':
            fail += 1
            continue

        try:
            song = genius.search_song(title=title, artist=artist)

            if song is None:
                print("Không tìm thấy bài")
                fail += 1
                continue

            lyric = song.lyrics

            if not lyric:
                fail += 1
                continue

            genius_year = extract_year_from_song(song)

            if metadata_year and genius_year:
                if metadata_year not in genius_year:
                    print(f"Không khớp năm: {metadata_year} vs {genius_year}")
                    fail += 1
                    continue

            lyric_clean = lyric.strip().replace("\r\n", " \\n ").replace("\n", " \\n ").replace("\r", " ")
            
            rows.append({
                "TRACK_ID": track_id,
                "TITLE": title,
                "ARTIST": artist,
                "YEAR": metadata_year,
                "LYRIC": lyric_clean
            })

            success += 1
            print("[OK]")

        except Exception as e:
            print("Lỗi:", e)
            fail += 1

        time.sleep(SLEEP_TIME)

    os.makedirs(os.path.dirname(OUTPUT_FILE), exist_ok=True)

    with open(OUTPUT_FILE, "w", newline="", encoding="utf-8-sig") as f:
        fieldnames = ["TRACK_ID", "TITLE", "ARTIST", "YEAR", "LYRIC"]
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)

    print("\n===== HOÀN TẤT =====")
    print("Thành công:", success)
    print("Thất bại:", fail)

if __name__ == "__main__":
    fetch_lyrics()