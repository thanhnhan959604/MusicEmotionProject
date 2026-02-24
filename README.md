<h1 align="center" font-size= 36px;><b>Xây dựng mô hình kết hợp đặc trưng âm thanh và văn bản trong bài toán phân loại nhạc theo cảm xúc</b></h1>


## II. Yêu cầu
* __Tổng quan:__ Mô hình được xây dựng dựa trên hai giai đoạn phá triển chính:
    * __Giai đoạn 1:__ Xây dựng mô hình MLP (Multi-layer Perception).
    * __Giai đoạn 2:__ Kết hợp đặc trưng văn bản để cải thiện độ chính xác.

### 1. Triển khai giai đoạn 1.
* __Data:__ Dữ liệu được sử dụng để huấn luyện mô hình là tập con __*autotagging_moodtheme*__ là tập con của kho dữ liệu audio __Jamendo__.
* __Feather__: BEATs là mô hình đã được sử dụng để trích xuất đặc trưng âm thanh dựa trên file audio đã được xữ lý. 

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

## III. Các thư viện hỗ trợ.
* __mutagen:__ lấy metadata từ audio