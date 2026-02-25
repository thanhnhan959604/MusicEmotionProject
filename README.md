<h1 align="center" font-size= 36px;><b>Xây dựng mô hình kết hợp đặc trưng âm thanh và văn bản trong bài toán phân loại nhạc theo cảm xúc</b></h1>

## I. Tổng quan.

## II. Xây dựng và phát triển.

## III. Nguyên tắt hoạt động.
### 1. Triển khai giai đoạn 1: filter_mood_dataset
* __Thư viện sử dụng:__
    * __pandas:__ Thư viện cốt lõi để thao tác và phân tích dữ liệu bảng (DataFrame). Nó được dùng để đọc file (CSV, TSV), lọc dữ liệu, tách chuỗi và lưu kết quả.
* __Nguyên lý hoạt đông:__
    * __Bước 1 - Khởi tạo danh sách nhãn mục tiêu (Mood list):__ Code đọc file `mood_tag.txt`
        * __Nguyên lý:__ Dùng vòng lặp để tách lấy phần `mode_name` (phần sau dấu ---) và lưu vào `mood_list`. Đây đóng vai trò là "bộ lọc" để chỉ giữ lại các cảm xúc hợp lệ
    * __Bước 2 - Xử lý file dữ liệu thô (TSV):__ File `autotagging_moodtheme.tsv` ban đầu được đọc như một cột duy nhất có tên là `full_line`.
        * __Kỹ thuật tách cột:__ Code sử dụng `.str.split("\t", n=5, expand=True)` để chia thành 6 cột tương ứng với: ID bài hát, ID nghệ sĩ, ID album, đường dẫn, thời lượng và các thẻ tag.
    * __Bước 3 - Lọc và chuẩn hoá Tags:__ Chương trình duyệt qua từng bài hát.
        * 1. Lấy chuỗi `TAG`và tách chúng ra thành từng tag riêng lẻ
        * 2. Với mỗi tag, kiểm tra xem có chứa dấu `---` hay không.
        * 3. Nếu có, tách lấy `lable`. nếu `lable` này nằm trong `mood_list` đã tạo ở bước 1, nó sẽ được giữ lại và định dạng lại thành `mood---lable`.
        * 4. __Điều kiện:__ chỉ những bài hát nào có ít nhất một mood hợp lệ mới được thêm vào danh sách kết quả `row`.
    * __Bước 4:__ Lưu kết quả
        * Cuối cùng, danh sách `row` được chuyển đổi ngược lại thành một DataFrame sạch sẽ và lưu xuống file `filter_mood.csv`
* __Đánh giá tổng quan:__
    * __Mục đích:__ Chuyển đổi dữ liệu từ dạng gắn thẻ tự động (thông dụng) sang dạng dữ liệu có cấu trúc, chỉ chứa các nhãn cảm xúc mong muốn.
    * __Ưu điểm:__ Cách xử lý `sorted(moods)` giúp dữ liệu đầu ra đồng nhất, không bị trùng lặp nhãn trên cùng một bài hát.
    * __Lưu ý:__ Việc xử lý có thể sẽ chậm nếu file dữ liệu quá lớn (lên đến hàng triệu dòng). Tuy nhiên, với dữ liệu metadata âm nhạc thông thường, cách này mang lại hiệu quả dễ đọc và bảo trì.
    
### 2. Triển khai giai đoạn 2.

### 3. Triển khai giai đoạn 3.

### 4. Triển khai giai đoạn 4.

### 5. Triển khai giai đoạn 5.

### 6. Triển khai giai đoạn 6.

## IV. Kết quả và đánh giá.

## V. Cấu trúc thư mục.
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

## VI. Các thư viện hỗ trợ.
* __mutagen:__ lấy metadata từ audio