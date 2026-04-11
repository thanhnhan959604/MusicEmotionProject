import os
from pathlib import Path

#lớp này chứa toàn bộ đường dẫn và cẩu hình của dự á
class PipelineConfig:
    
    #lấy đường dẫn gốc
    ROOT_DIR = Path(__file__).resolve().parent.parent.parent
    
    #các thư mục chính
    DATA_DIR = ROOT_DIR/"data"
    CACHE_DIR = ROOT_DIR/"cache"
    AUDIO_DIR = DATA_DIR/"audio_previews"
    PROCESSED_DIR = DATA_DIR/"processed"
    
    os.makedirs(DATA_DIR, exist_ok=True)
    os.makedirs(CACHE_DIR, exist_ok=True)
    os.makedirs(AUDIO_DIR, exist_ok=True)
    os.makedirs(PROCESSED_DIR, exist_ok=True)
    
    
    #đường dẫn các file đầu ra theo pipeline
    
    # step01: crawl IDs
    TRACK_IDS_FILE = CACHE_DIR/"track_ids.txt"
    CRAWLED_ARTISTS_FILE = CACHE_DIR/"crawled_artists.txt"

    # step02: fetch metadata & audio features
    RAW_METADATA_FILE = DATA_DIR / "step2_raw_metadata.csv"
    AUDIO_FEATURES_FILE = DATA_DIR / "step2_audio_features.csv"

    # step03: clean & dedup
    CLEANED_DATA_FILE = DATA_DIR / "step3_cleaned.csv"

    # step04: lyrics & filter Vietnamese
    LYRICS_RAW_FILE = DATA_DIR / "step4_lyrics_raw.csv"
    VIETNAMESE_ONLY_FILE = DATA_DIR / "step4_vietnamese_only.csv"

    # step05: master dataset (merge)
    MASTER_DATASET_FILE = DATA_DIR / "step5_master_dataset.csv"

    # step06: download log (preview audio)
    DOWNLOAD_LOG_FILE = DATA_DIR / "step6_download_log.csv"

    # step07: train-ready dataset
    TRAIN_READY_FILE = DATA_DIR / "step7_train_ready.csv"
    
    #từ khoá mầm ban đầu
    SEED_KEYWORDS = ["Sơn Tùng M-TP", "Đen Vâu", "Hoàng Thùy Linh", "HIEUTHUHAI", "B Ray", "V-pop"]
    TARGET_COUNT = 10000

    # whitelist nghệ sĩ Việt
    KNOWN_VIET_ARTISTS = {
        # Rapper / Hip-hop
        "mck",
        "rpt mck",
        "wxrdie",
        "low g",
        "lil wuyn",
        "gonzo",
        "datmaniac",
        "2pillz",
        "coldzy",
        "manbo",
        "obito",
        "tlinh",
        "hurrykng",
        "b ray",
        "bray",
        "lk",
        "sol7",
        "nick*",
        # Pop / V-pop
        "jack",
        "hieuthuhai",
        "mono",
        "erik",
        "amee",
        "min",
        "grey d",
        "justatee",
        "rhyder",
        "phuong ly",
        "noo phuoc thinh",
        "vu cat tuong",
        "ho quang hieu",
        "ho ngoc ha",
        "my tam",
        "dam vinh hung",
        "tuan hung",
        "le quyen",
        # Nhom nhac
        "uni5",
        "365daband",
        "monstar",
    }