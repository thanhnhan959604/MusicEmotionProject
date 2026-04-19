import os
from pathlib import Path


class PipelineConfig:
    # THƯ MỤC GỐC
    ROOT_DIR = Path(__file__).resolve().parent.parent.parent
    DATA_DIR = ROOT_DIR / "data"
    CACHE_DIR = ROOT_DIR / "cache"
    AUDIO_DIR = DATA_DIR / "audio_previews"

    # Tạo thư mục cần thiết ngay khi import (chỉ những thư mục thực sự dùng)
    os.makedirs(DATA_DIR,  exist_ok=True)
    os.makedirs(CACHE_DIR, exist_ok=True)
    os.makedirs(AUDIO_DIR, exist_ok=True)

    # FILE ĐẦU RA THEO TỪNG BƯỚC

    # Step 1 – Thu thập Track IDs
    CRAWLED_TRACKS_CSV = DATA_DIR  / "step1_tracks.csv"
    TRACK_IDS_FILE = CACHE_DIR / "track_ids.txt"
    CRAWLED_ARTISTS_FILE = CACHE_DIR / "crawled_artists.txt"

    # Step 2 – Fetch metadata & audio features
    RAW_METADATA_FILE = DATA_DIR / "step2_raw_metadata.csv"
    AUDIO_FEATURES_FILE = DATA_DIR / "step2_audio_features.csv"

    # Step 3 – Làm sạch & xoá trùng
    CLEANED_DATA_FILE = DATA_DIR / "step3_cleaned.csv"

    # Step 4 – Lyrics & lọc tiếng Việt
    LYRICS_RAW_FILE = DATA_DIR / "step4_lyrics_raw.csv"
    VIETNAMESE_ONLY_FILE = DATA_DIR / "step4_vietnamese_only.csv"

    # Step 5 – Gộp master dataset
    MASTER_DATASET_FILE = DATA_DIR / "step5_master_dataset.csv"

    # Step 6 – Tải audio preview
    DOWNLOAD_LOG_FILE     = DATA_DIR / "step6_download_log.csv"

    # Step 7 – Đồng bộ & train-ready
    TRAIN_READY_FILE = DATA_DIR / "step7_train_ready.csv"
    QUARANTINE_AUDIO_DIR = DATA_DIR / "step7_quarantine_audio"
    QUARANTINE_CSV = DATA_DIR / "step7_quarantine_csv.csv"

    # THAM SỐ PIPELINE
    TARGET_COUNT = 100_000

    # TỪ KHOÁ NHẬN DIỆN LIÊN KHÚC (dùng ở Step 4)
    LIENKHUC_NAME_KEYWORDS = [
        # Liên khúc / Medley / Mashup
        "liên khúc", "lien khuc",
        "mashup", "mash up", "mash-up",
        "medley",
        # Viết tắt LK (có khoảng trắng để tránh lọc nhầm tên Rapper)
        "lk ", " lk", "[lk]", "(lk)", "lk.", "lk-",

        # Nhạc sàn / DJ Set / Mix dài
        "nonstop", "non-stop", "non stop",
        "megamix", "mega mix",
        "mixtape", "mix tape",
        "live set", "dj set", "djset",
        "vietmix", "viet mix", "club mix",
        "vina house nonstop", "vinahouse nonstop",
        " mix ",   # khoảng trắng để không lọc nhầm "remix" bài đơn lẻ

        # Tuyển tập / Tổng hợp
        "tuyển tập", "tuyen tap",
        "tổng hợp", "tong hop",
        "chọn lọc", "chon loc",
        "the best of", "best of",
        "greatest hits", "collection",

        # Cụm từ gom nhiều bài
        "những bài hát", "nhung bai hat",
        "những ca khúc", "nhung ca khuc",
        "ca khúc hay nhất", "ca khuc hay nhat",
        "tuyệt phẩm", "tuyet pham",
        "đỉnh cao", "dinh cao",
        "để đời", "de doi",

        # Nguyên Album / CD dài
        "full album", "album full",
        "trọn bộ", "tron bo",
        "cd1", "cd 1", "cd2", "cd 2", "full cd",

        # Nhạc sống / kẹo kéo
        "nhạc sống", "nhac song",
        "kẹo kéo", "keo keo",
    ]

    # SEED KEYWORDS – Danh sách ~200 nghệ sĩ & từ khoá nhạc Việt (Step 1)
    SEED_KEYWORDS = [
        "Sơn Tùng M-TP", "Đen Vâu", "Hoàng Thùy Linh", "HIEUTHUHAI", "B Ray", "Jack - J97",
        "Phan Mạnh Quỳnh", "Bích Phương", "Tóc Tiên", "Min", "Erik", "Đức Phúc", "Hòa Minzy",
        "Trúc Nhân", "Karik", "Binz", "JustaTee", "Rhymastic", "Suboi", "Wowy", "MCK", "tlinh",
        "Obito", "Low G", "Vũ.", "Ngọt", "Chillies", "Cá Hồi Hoang", "Da LAB", "Thái Đinh",
        "Lê Cát Trọng Lý", "Amee", "Suni Hạ Linh", "Phùng Khánh Linh", "Grey D", "MONO",
        "Tăng Duy Tân", "Wren Evans", "Myra Trần", "Văn Mai Hương", "Trung Quân Idol",
        "Noo Phước Thịnh", "Đông Nhi", "Ông Cao Thắng", "Bảo Anh", "Hương Tràm", "Chi Pu",
        "Soobin Hoàng Sơn", "Cầm", "Phương Ly", "Thùy Chi", "Mỹ Tâm", "Hà Anh Tuấn", "Lệ Quyên",
        "Tuấn Hưng", "Bằng Kiều", "Hồ Ngọc Hà", "Đàm Vĩnh Hưng", "Quang Dũng", "Thanh Thảo",
        "Đan Trường", "Cẩm Ly", "Lam Trường", "Phương Thanh", "Thu Minh", "Ưng Hoàng Phúc",
        "Phạm Quỳnh Anh", "Khổng Tú Quỳnh", "Ngô Kiến Huy", "Miu Lê", "Trịnh Thăng Bình",
        "Lou Hoàng", "Only C", "Khắc Việt", "Vũ Duy Khánh", "Châu Khải Phong", "Hồ Quang Hiếu",
        "Phan Đinh Tùng", "Quang Vinh", "Bảo Thy", "Đinh Hương", "Thảo Trang", "Uyên Linh",
        "Quốc Thiên", "Hương Giang", "Phạm Hồng Phước", "Tiên Cookie", "Tiên Tiên", "Trang",
        "Nhạc Trẻ", "V-Pop", "Nhạc Việt Nam", "Rap Việt", "Indie Việt", "Nhạc Lofi Việt",
        "Acoustic Việt", "Remix Việt", "Vinahouse", "Bolero", "Nhạc Xưa", "Nhạc Trữ Tình",
        "Đạt G", "Du Uyên", "Hoa Vinh", "Jack và K-ICM", "K-ICM", "Masew", "Pháo", "Xesi",
        "Ricky Star", "Seachains", "Dế Choắt", "Lăng LD", "Yuno Bigboi", "G-Ducky", "Tage",
        "Lil Wuyn", "B-Wine", "16 Typh", "Gonzo", "Thành Draw", "Orijinn", "RPT JasonDilla",
        "Vsoul", "Robber", "TGSN", "Wxrdie", "Nger", "Right", "Mikelodic",
        "HuyR", "Đình Dũng", "Thái Học", "Thương Võ", "Hương Ly", "Gia Huy Singer",
        "Quân A.P", "Anh Tú", "LyLy", "Châu Đăng Khoa", "Orange", "Sofia", "Khói",
        "Dick", "Huỳnh James", "Pjnboys", "Nguyễn Trần Trung Quân", "Denis Đặng",
        "Thiều Bảo Trâm", "Thiều Bảo Trang", "Ali Hoàng Dương", "Juky San", "Thịnh Suy",
        "Kiên", "Táo", "Đạt Maniac", "Namling", "G5R Squad", "Jombie", "Lục Huy",
        "Cody", "Uni5", "Lip B", "MONSTAR", "OPlus", "Ngọt Band", "Bức Tường", "Microwave",
        "Ngũ Cung", "Cát Tiên", "Lâm Chấn Khang", "Lâm Chấn Huy", "Châu Việt Cường",
    ]