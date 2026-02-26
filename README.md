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
* __Yêu cầu hệ thống & cài đặc:__
    * Trước khi chạy script, cần cài đặc Python (phiên bản 3.7+).
    * __Các thư viện cần thiết:__ `pip install pandas requests tqdm`
* __Cấu trúc dữ liệu thư mục:__
    * `../data/intermediate/`: Chứa file filter_moode.csv.
    * `../data/raw/audio_mood`: Nơi script sẽ tự động tạo và lưu các file nhạc tải về.
* __Quản lý tài nguyên và luồng:__
    * `ThreadPoolExecutor(max_workers=10)`: Cho phép chạy tối đa 10 nhiệm vụ tải cùng một lúc để tăng tốc độ.
    * `Semaphore(3)`: Thiết lập một chốt chặn kiểm soát truy cập. Dù có 10 luồng đang chạy, nhưng tại một thời điểm chỉ có tối đa 3 luồng được phép gửi yêu cầu tải thực tế để tránh bị API chặn (Rate Limiting)
* __Nguyên lý hoạt động:__
    * __Bước 1 - Xử lý ID:__ Chuyển đổi định dạng ID từ chuỗi sang số nguyên để khớp với cấu trúc API.
    * __Bước 2 - Kiểm tra trùng lặp:__ Nếu file đã tồn tại trong thư mục lưu trữ, script sẽ bỏ qua để tiết kiệm thời gian và băng thông.
    * __Bước 3 - Tải dạng Stream:__ Sử dụng `stream = true` và `iter_content` để ghi file theo từng khối (chunk). Cách này giúp máy không bị tràn bộ nhớ RAM khi tải các file nhạc dung lượng lớn.
    * __Bước 4 - Độ trễ:__ Lệnh `time.sleep(0.3)` giúp tạo một khoảng ghi ngắn giữa các yêu cầu giữ cho kết nối ổn định hơn.
* __Cách sử dụng:__
    * __1.__ Đảm bảo file `filter_mood.csv` có cột tên là `TRACK_ID`
    * __2.__ Thay đổi `client_id = "c2946d30"` nếu bạn có mã ứng dụng riêng từ Jamendo Developer Portal.
    * __3.__ Chạy lệnh script bằng: `python ten_file_cua_ban.py`
    * __4.__ Theo dõi tiến trình trên màn hình để biết dung lượng đã tải và thời gian dự kiến còn lại.
* __Một số lưu ý quan trọng:__
    * __Giới hạn API:__ Nếu lỗi 429 (Too Many Requests), hãy giảm số lượng trong `semaphore(3)` xuống thấp hơn hoặc tăng `time.sleep()`
    * __Bản quyền:__ Hãy đảm bảo tuân thủ các điều khoản sử dụng của Jamendo đối với dữ liệu âm thanh được tải về.
    * __Kết nối mạng:__ Nếu mạng không ổn định, tham số `timeout=30` sẽ tự động ngắt kết nối bị treo sau 30 giây để chuyển sang bài tiếp theo.
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