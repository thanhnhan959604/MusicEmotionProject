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
    
    #bước 1: 
    TRACK_IDS_FILE = CACHE_DIR/"track_ids.txt"
    CRAWLED_ARTISTS_FILE = CACHE_DIR/"crawled_artists.txt"
    
    #từ khoá mầm ban đầu
    SEED_KEYWORDS = ["Sơn Tùng M-TP", "Đen Vâu", "Hoàng Thùy Linh", "HIEUTHUHAI", "B Ray", "V-pop"]
    TARGET_COUNT = 10000