import os
import csv
from mutagen.mp3 import MP3
from mutagen.easyid3 import EasyID3
from mutagen.id3 import ID3NoHeaderError


AUDIO_FOLDER = "data/raw/audio"
OUTPUT_FILE = "data/metadata/audio_metadata.csv"


#Kiểm tra thư mục
def check_audio_folder_exists():
    if not os.path.exists(AUDIO_FOLDER):
        print("[-] Thư mục audio không tồn tại: ", AUDIO_FOLDER)
        return False
    return True

#lấy danh sách file mp3 trong thư mục audio
def get_mp3_files():
    mp3_files = []
    files = os.listdir(AUDIO_FOLDER)
    for file_name in files:
        if file_name.endswith(".mp3"):
            mp3_files.append(file_name)
    return mp3_files

#đọc metadata
def extract_metadata_from_file(file_path):
    title = ""
    artist = ""
    album = ""
    year = ""
    genre = ""
    duration = ""
    bitrate = ""

    try:
        #lấy thông tin
        audio = MP3(file_path)
        duration = str(int(audio.info.length))
        bitrate = str(int(audio.info.bitrate / 1000)) + "kbps"

        #đọc ID3 tag
        try:
            tags = EasyID3(file_path)

            if "title" in tags:
                title = tags["title"][0]
            if "artist" in tags:
                artist = tags["artist"][0]
            if "album" in tags:
                album = tags["album"][0]
            if "date" in tags:
                year = tags["date"][0]
            if "genre" in tags:
                genre = tags["genre"][0]
        except ID3NoHeaderError:
            print("[INFO] File không có ID3:", file_path)
    except Exception as e:
        print("[ERROR] Không đọc được file: ", e)

    return title, artist, album, year, genre, duration, bitrate

#tạo file csv
def create_metadata_csv():
    print("BẮT ĐẦU TRÍCH XUẤT METADATA")

    if not check_audio_folder_exists():
        return
    
    mp3_files = get_mp3_files()

    print("[INFO] Số file mp3 tìm thấy: ", len(mp3_files))

    if len(mp3_files) == 0:
        print("[-] Không có file mp3 nào")
        return
    
    rows = []
    for file_name in mp3_files:
        file_path = os.path.join(AUDIO_FOLDER, file_name)
        track_id = file_name.replace(".mp3", "")
        print("Xử lý: ", track_id)
        
        title, artist, album, year, genre, duration, bitrate = extract_metadata_from_file(file_path)

        row = {
            "TRACK_ID": track_id,
            "TITLE": title,
            "ARTIST": artist,
            "ALBUM": album,
            "YEAR": year,
            "GENRE": genre,
            "DURATION": duration,
            "BITRATE": bitrate
        }
        rows.append(row)
    
    #tạo thư mục metadata nếu chưa có
    os.makedirs("data/metadata", exist_ok=True)

    #ghi CSV
    with open(OUTPUT_FILE, mode="w", newline="", encoding="utf-8-sig") as f:
        fieldnames = [
            "TRACK_ID",
            "TITLE",
            "ARTIST",
            "ALBUM",
            "YEAR",
            "GENRE",
            "DURATION",
            "BITRATE"
        ]
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()

        for row in rows:
            writer.writerow(row)
    print("\n [SUCCESS] Đã tạo file: ", OUTPUT_FILE)
    print("Tổng số bài xử lý: ", len(rows))


if __name__ == "__main__":
    create_metadata_csv()