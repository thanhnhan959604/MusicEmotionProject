import os
import csv
import pandas as pd
import lyricsgenius
from concurrent.futures import ThreadPoolExecutor, as_completed

# ===== CONFIG =====
GENIUS_TOKEN = "YOUR_TOKEN_HERE"
METADATA_FILE = "data/metadata/audio_metadata.csv"
OUTPUT_FILE = "data/lyrics/genius_lyrics.csv"
MAX_WORKERS = 10


def load_metadata():
    if not os.path.exists(METADATA_FILE):
        print("Không tìm thấy metadata file")
        return None

    df = pd.read_csv(METADATA_FILE, encoding="utf-8-sig")
    df["TRACK_ID"] = df["TRACK_ID"].astype(str)

    clean_years = []
    for y in df["YEAR"]:
        if pd.notnull(y):
            try:
                clean_years.append(str(int(float(y))))
            except:
                clean_years.append("")
        else:
            clean_years.append("")

    df["YEAR"] = clean_years
    return df


def create_client():
    genius = lyricsgenius.Genius(
        GENIUS_TOKEN,
        timeout=15,
        sleep_time=1,
        retries=3
    )

    genius.verbose = False
    genius.remove_section_headers = True
    genius.skip_non_songs = True
    genius.excluded_terms = ["(Remix)", "(Live)"]

    return genius


def extract_year_from_song(song):
    try:
        if hasattr(song, "_body"):
            body = song._body
            if "release_date_components" in body:
                comp = body["release_date_components"]
                if comp and "year" in comp and comp["year"]:
                    return int(comp["year"])
    except:
        return None

    return None


def process_row(row):
    genius = create_client()  # mỗi thread tạo client riêng

    track_id = row["TRACK_ID"]
    title = str(row["TITLE"]).strip()
    artist = str(row["ARTIST"]).strip()
    metadata_year = row["YEAR"]

    if not title or not artist:
        return None

    try:
        song = genius.search_song(title=title, artist=artist)

        if song is None:
            return None

        lyric = song.lyrics
        if not lyric:
            return None

        genius_year = extract_year_from_song(song)

        if metadata_year and genius_year:
            if int(metadata_year) != genius_year:
                return None

        return {
            "TRACK_ID": track_id,
            "TITLE": title,
            "ARTIST": artist,
            "YEAR": metadata_year,
            "LYRIC": lyric.strip()
        }

    except:
        return None


def fetch_lyrics():

    print("===== FETCH LYRICS FROM GENIUS (10 THREADS) =====")

    df_meta = load_metadata()
    if df_meta is None:
        return

    rows = []
    success = 0
    fail = 0

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:

        futures = []

        for _, row in df_meta.iterrows():
            futures.append(executor.submit(process_row, row))

        for future in as_completed(futures):
            result = future.result()

            if result:
                rows.append(result)
                success += 1
                print("[OK]", result["TRACK_ID"])
            else:
                fail += 1

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