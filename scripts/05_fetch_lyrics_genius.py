import os
import csv
import time
import pandas as pd
import lyricsgenius


# ===== CONFIG =====
GENIUS_TOKEN = "XV3VmrcJ5XpX3m36vaGrdAxxbcl7xhw6i1C2HJowheEUYU0X2o84yXhsb1HkIuvl"
METADATA_FILE = "data/metadata/audio_metadata.csv"
OUTPUT_FILE = "data/lyrics/genius_lyrics.csv"
SLEEP_TIME = 2


def load_metadata():
    if not os.path.exists(METADATA_FILE):
        print("Không tìm thấy metadata file")
        return None

    df = pd.read_csv(METADATA_FILE, encoding="utf-8-sig")

    df["TRACK_ID"] = df["TRACK_ID"].astype(str)

    # ===== FIX YEAR FLOAT -> INT STRING =====
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

    genius.verbose = True
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


def fetch_lyrics():

    print("===== FETCH LYRICS FROM GENIUS =====")

    df_meta = load_metadata()
    if df_meta is None:
        return

    genius = create_client()

    rows = []
    success = 0
    fail = 0

    for _, row in df_meta.iterrows():

        track_id = row["TRACK_ID"]
        title = str(row["TITLE"]).strip()
        artist = str(row["ARTIST"]).strip()
        metadata_year = row["YEAR"]  # đã clean ở load

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

            genius_year = extract_year_from_song(song)

            # ===== SO SÁNH NĂM DẠNG INT =====
            if metadata_year and genius_year:

                if int(metadata_year) != genius_year:
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