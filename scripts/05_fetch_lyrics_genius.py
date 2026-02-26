import os
import csv
import time
import pandas as pd
import lyricsgenius
from tqdm import tqdm


# ===== CONFIG =====
GENIUS_TOKEN = "FL-n454ZIpvBOZuUFDOY5t40U0Kp6DKCHnpwo0y1sWNythe870GQ-Furh6SEK8LB"
METADATA_FILE = r"D:\KY_6\DA2\MusicEmotionProject\data\metadata\splits\audio_metadata_part_2.csv"
OUTPUT_FILE = "data/lyrics/genius_lyrics_2.csv"
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

    print("===== FETCH LYRICS FROM GENIUS =====")

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

        print("Đang xử lý:", track_id)

        if not title or not artist:
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

            # ===== LẤY NĂM TỪ JSON =====
            genius_year = extract_year_from_song(song)

            # ===== SO SÁNH NĂM =====
            if metadata_year and genius_year:
                if metadata_year not in genius_year:
                    print("Không khớp năm:", metadata_year, "vs", genius_year)
                    fail += 1
                    continue

            rows.append({
                "TRACK_ID": track_id,
                "TITLE": title,
                "ARTIST": artist,
                "YEAR": metadata_year,
                "LYRIC": lyric.strip()
            })

            success += 1
            print("[OK]")

        except Exception as e:
            print("Lỗi:", e)
            fail += 1

        time.sleep(SLEEP_TIME)

    # ===== SAVE FILE =====
    os.makedirs("data/lyrics", exist_ok=True)

    with open(OUTPUT_FILE, "w", newline="", encoding="utf-8-sig") as f:
        fieldnames = ["TRACK_ID", "TITLE", "ARTIST", "YEAR", "LYRIC"]
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)

    print("\nHoàn tất")
    print("Thành công:", success)
    print("Thất bại:", fail)


if __name__ == "__main__":
    fetch_lyrics()