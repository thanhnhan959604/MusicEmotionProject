# ĐỒ ÁN HỌC KỲ II
## I. Cấu trúc thư mục.
```Text
MusicEmotionProject/
│
├── data/
│   │
│   ├── raw/
│   │   ├── autotagging_moodtheme.tsv        # dataset gốc
│   │   ├── mood_tag.txt                     # file tag cảm xúc bạn trích ra
│   │   │
│   │   └── audio/                           # mp3 đã tải
│   │       ├── track_0002263.mp3
│   │       └── ...
│   │
│   ├── intermediate/
│   │   ├── mood_filtered_dataset.csv        # (rename từ filter_mood.csv)
│   │
│   ├── metadata/
│   │   ├── audio_metadata.csv
│   │
│   ├── lyrics/
│   │   ├── jamendo_lyrics.csv
│   │   ├── genius_lyrics.csv
│   │
│   └── processed/
│       ├── final_training_dataset.csv
│
├── scripts/
│   ├── 01_filter_mood_dataset.py        # (rename từ filter_tag.py)
│   ├── 02_download_audio.py             # (rename từ download_audio.py)
│   ├── 03_extract_audio_metadata.py
│   ├── 04_fetch_lyrics_jamendo.py
│   ├── 05_fetch_lyrics_genius.py
│   └── 06_train_model.py
│
├── models/
├── logs/
├── requirements.txt
└── README.md
```

## II. Yêu cầu
__Tổng quan:__ Xây dựng mô hình nhận diện cảm xúc âm nhạc kết hợp trích xuất lyric từ audio.
__Mô hình:__