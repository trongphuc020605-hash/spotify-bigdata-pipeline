# ĐỒ ÁN BIG DATA: PHÂN TÍCH XU HƯỚNG ÂM NHẠC TRÊN SPOTIFY (2017 - 2023)

- **Trường đào tạo**: Trường Đại học Công Thương TP.HCM (HUIT)
- **Học phần**: Đồ án Big Data
- **Nhóm sinh viên thực hiện**:
  1. **Nguyễn Hà Trọng Phúc** (Nhóm trưởng - MSSV: `2001230686`)
  2. **Lê Đông Triều**(MSSV:2001230984)
  3. **Nguyễn Vũ Ngọc Nam**(MSSV:2001230545)
- **Đề tài**: *Phân tích xu hướng âm nhạc trên Spotify giai đoạn 2017 - 2023*
- **Nguồn dữ liệu**: [Top 200 Spotify Songs Dataset (Kaggle - brunoalarcon123)](https://www.kaggle.com/datasets/brunoalarcon123/top-200-spotify-songs-dataset)

---

## 1. Tổng Quan Hệ Thống

Dự án cung cấp một hệ sinh thái Dashboard phân tích Big Data hoàn chỉnh gồm 2 giải pháp trực quan hóa:

### 1.1. Python Streamlit Interactive Dashboard (`app.py`)
- **Công nghệ**: `Python 3.13`, `Streamlit`, `Pandas`, `Plotly`, `Scikit-Learn`.
- **Giao diện**: Chuẩn Spotify Dark Theme (`#121212`, `#1DB954`).
- **Khả năng xử lý**: Hỗ trợ tập dữ liệu lớn (> 650.000 dòng) với cơ chế Caching `@st.cache_data`.
- **Phân hệ chức năng (6 Tabs)**:
  1. 📊 **Tổng Quan & KPI**: Thống kê số lượng bài hát, nghệ sĩ, điểm xếp hạng, Radar hồ sơ âm thanh chuẩn, phân bố Violin Plot.
  2. 📈 **Xu Hướng Thời Gian 2017 - 2023**: Biến động các đặc trưng âm học theo năm/tháng, xu hướng ca khúc ngắn lại (Song Duration), phân tích tác động năm COVID (2020).
  3. 👑 **Bảng Vàng Nghệ Sĩ & Ca Khúc**: Top nghệ sĩ thống trị, bài hit nhiều điểm nhất, tính năng độc đáo **So sánh Đối đầu 2 Nghệ Sĩ (Head-to-Head Comparison)**.
  4. 🌍 **Phân Tích Địa Lý & Thị Trường**: Tỷ trọng xuất khẩu âm nhạc theo Châu lục và Quốc gia của nghệ sĩ.
  5. 🔬 **Machine Learning & Tương Quan Âm Học**: Ma trận tương quan Heatmap, Mô hình phân cụm **K-Means Clustering** thành 4 phong cách âm nhạc (*Party Hits, Chill Vibes, Melancholic, Rap/Hip-Hop*) kèm trực quan hóa không gian PCA.
  6. 🔍 **Tra Cứu Dữ Liệu & Xuất Báo Cáo**: Tìm kiếm ca khúc, bộ lọc nhiều chiều, xuất dữ liệu lọc ra file CSV.

### 1.2. Standalone Web Dashboard (`index.html`)
- **Công nghệ**: `HTML5`, `Tailwind CSS`, `Chart.js`, `PapaParse`, `FontAwesome`.
- **Ưu điểm**: Mở trực tiếp trên mọi trình duyệt (Chrome, Edge, Firefox) mà **không cần cài đặt môi trường**.
- **Tính năng**: Tích hợp sẵn dữ liệu benchmark 2017 - 2023; hỗ trợ kéo-thả (Drag & Drop) tệp CSV với thuật toán tự động nhận diện dấu phân cách (`;` hoặc `,`).

---

## 2. Hướng Dẫn Khởi Chạy Hệ Thống

### Cách 1: Khởi Chạy Nhanh 1-Click Bằng File Batch (.bat) trên Windows (Khuyên Dùng)
- **Mở Streamlit Dashboard**: Nhấp đúp chuột vào file **`run_dashboard.bat`** (Tự động giải phóng cổng 8501 nếu bị chiếm và mở trình duyệt tại `http://localhost:8501`).
- **Chạy PySpark K-Means & RFM**: Nhấp đúp vào **`run_spark_kmeans.bat`** để chạy thuật toán phân cụm trên Spark.
- **Chạy Kafka Streaming Producer**: Nhấp đúp vào **`run_kafka_producer.bat`** để mô phỏng phát luồng sự kiện realtime.
- **Mở Web HTML Dashboard Trực Tiếp**: Nhấp đúp vào **`open_web_dashboard.bat`** hoặc mở trực tiếp `index.html`.

---

### Cách 2: Khởi Chạy Bằng Anaconda Prompt (Dòng Lệnh CLI)

Mở **Anaconda Prompt** trên Windows, di chuyển vào thư mục dự án:
```cmd
cd C:\Users\tphuc\Downloads\web_dashboard_spotify
```

Thực thi các lệnh tương ứng theo nhu cầu:
1. **Khởi động Streamlit Web Dashboard**:
   ```cmd
   streamlit run app.py
   ```
   *(Hoặc gõ: `python -m streamlit run app.py --server.port 8501`)*

2. **Chạy Thuật toán PySpark K-Means & RFM (Bước 6 & 7)**:
   ```cmd
   python pyspark_rfm_kmeans.py --input spotify_cleaned.parquet --output ./kmeans_results.parquet
   ```

3. **Chạy Kafka Streaming Producer (Bước 8)**:
   ```cmd
   python kafka_producer.py --file spotify_top200_sample.csv --min-delay 1.0 --max-delay 2.0
   ```

4. **Chạy lại Tiền xử lý & Làm sạch dữ liệu (Bước 4)**:
   ```cmd
   python preprocess.py
   ```

*(Nếu dùng PowerShell thông thường, bạn có thể gọi trực tiếp Python Anaconda: `& "C:\Users\tphuc\anaconda3\python.exe" -m streamlit run app.py`)*

---

### Cách 3: Khởi Chạy Trên Môi Trường Linux Lab Của Trường (Ailogy Cloud / Hadoop Cluster)

Mở **Terminal thực hành** trên Linux Lab (`/courses/2/projects/3/students/2001230686`):
```bash
# 1. Chạy PySpark Job trên cụm HDFS:
spark-submit \
  --master local[*] \
  --deploy-mode client \
  pyspark_rfm_kmeans.py \
  --input hdfs://localhost:9000/spotify/processed/spotify_cleaned.parquet \
  --output hdfs://localhost:9000/spotify/output/kmeans_results.parquet

# 2. Chạy Kafka Producer:
python3 kafka_producer.py --file spotify_top200_sample.csv

# 3. Chạy kiểm tra dữ liệu nhận về qua Kafka Consumer:
kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic spotify_streaming --from-beginning

# 4. Kích hoạt Airflow DAG:
cp spotify_pipeline_dag.py ~/airflow/dags/
airflow dags test spotify_bigdata_analytics_pipeline 2023-01-01
```

---

## 3. Cấu Trúc Thư Mục Dự Án Sau Khi Tối Ưu Hóa

```text
web_dashboard_spotify/
│
├── 📦 archive_backup_files.zip  # Tệp nén lưu trữ dữ liệu thô gốc & script phụ (37.3 MB)
│
├── ⚙️ PIPELINE MÃ NGUỒN CHUẨN 9 BƯỚC:
│   ├── preprocess.py            # [Bước 4] Tiền xử lý & xuất Parquet chuẩn ms
│   ├── pyspark_rfm_kmeans.py    # [Bước 6 & 7] Thuật toán PySpark RFM & K-Means trên HDFS
│   ├── kafka_producer.py        # [Bước 8] Producer mô phỏng streaming realtime
│   └── spotify_pipeline_dag.py  # [Bước 9] Apache Airflow DAG điều phối tự động @daily
│
├── 💾 DỮ LIỆU ĐÃ TỐI ƯU HÓA:
│   ├── spotify_cleaned.parquet  # Dữ liệu sạch dạng cột Parquet (HDFS Silver Layer)
│   ├── kmeans_results.parquet   # Kết quả phân cụm Machine Learning từ Spark (HDFS Gold Layer)
│   ├── spotify_top200_sample.csv# 25.000 bản ghi mẫu phục vụ phát luồng Kafka
│   ├── dashboard_summary.json   # Tệp thống kê tổng hợp phục vụ giao diện
│   └── kafka_stream_buffer.json # Bộ đệm nhận sự kiện streaming theo thời gian thực
│
├── 🖥️ TRỰC QUAN HÓA & DASHBOARD:
│   ├── app.py                   # Streamlit Web Dashboard 7 Tabs hoàn chỉnh
│   └── index.html               # Web HTML Standalone phụ trợ
│
├── 🚀 TỆP THỰC THI NHANH 1-CLICK:
│   ├── run_dashboard.bat        # Bật Streamlit Dashboard (Tự dọn cổng 8501)
│   ├── run_spark_kmeans.bat     # Bật PySpark K-Means & RFM
│   ├── run_kafka_producer.bat   # Bật Kafka Streaming Producer
│   └── open_web_dashboard.bat   # Mở Web HTML
│
└── 📄 MINH CHỨNG & TÀI LIỆU:
    ├── README.md                # Tài liệu hướng dẫn & thuyết minh đề tài
    └── quy_trinh_lab_9_buoc.png # Ảnh minh chứng quy trình 9 bước trên lab
```


---

## 3. Kiến Trúc Data Pipeline Chuẩn Big Data (Quy Trình 9 Bước)

| Bước | Tên Bước | Công Nghệ | Mô Tả Thực Hiện |
| :---: | :--- | :---: | :--- |
| **1** | Chuẩn bị HDFS | Hadoop HDFS | Tạo cấu trúc thư mục `/spotify/raw`, `/spotify/processed`, `/spotify/output`. |
| **2** | Upload dữ liệu | HDFS CLI | Đưa tập dữ liệu thô `Spotify_Dataset_V3.csv` lên HDFS raw. |
| **3** | Đọc bằng Spark | PySpark SQL | Khởi tạo SparkSession kết nối và đọc dữ liệu phân tán. |
| **4** | Làm sạch dữ liệu | Python / Spark | Chuẩn hóa đặc trưng âm học, xử lý missing value, xuất `spotify_cleaned.parquet`. |
| **5** | Spark SQL | Spark SQL | Truy vấn tổng hợp xu hướng âm nhạc, tính toán KPI theo năm/tháng. |
| **6** | RFM & K-Means | PySpark MLlib | Tính ma trận RFM âm nhạc và gom cụm 4 phong cách bài hát (`pyspark_rfm_kmeans.py`). |
| **7** | Lưu kết quả | Parquet / HDFS | Ghi kết quả phân tích vào HDFS `hdfs://localhost:9000/spotify/output/kmeans_results.parquet`. |
| **8** | Kafka Ingest | Apache Kafka | Mô phỏng luồng phát trực tiếp ca khúc realtime vào topic `spotify_streaming` (`kafka_producer.py`). |
| **9** | Airflow DAG | Apache Airflow | Tự động hóa và điều phối toàn bộ pipeline định kỳ hàng ngày (`spotify_pipeline_dag.py`). |


---

## 4. Các Phát Hiện Đắt Giá (Key Insights Khi Thuyết Minh Trước Hội Đồng)

1. **Xu hướng Danceability & Energy thống trị**:
   - Chỉ số khả năng nhảy múa trung bình liên tục tăng từ **71.6% (2017)** lên trên **77.2% (2023)**.
   - Các ca khúc thành công trên Top 200 ngày càng có nhịp điệu dồn dập, tiết tấu rõ ràng, chịu ảnh hưởng mạnh mẽ từ xu hướng video ngắn (TikTok, Instagram Reels).

2. **Hiện tượng sụt giảm cảm xúc (Valence Drop) năm 2020**:
   - Chỉ số tích cực/vui tươi (Valence) giảm sâu xuống mức **49.88%** trong năm 2020 (đỉnh điểm đại dịch COVID-19 toàn cầu) khi người nghe tìm kiếm âm nhạc lắng đọng, tâm trạng (Melancholic, Bedroom Pop) và tăng trở lại từ năm 2021.

3. **Ca khúc ngày càng ngắn lại (Shrinking Song Duration)**:
   - Thời lượng trung bình của bài hát lọt Top 200 giảm từ **3.66 phút (2017)** xuống chỉ còn **3.17 phút (2023)** (giảm gần 30 giây mỗi bài). Nguyên nhân là do thuật toán trả phí của Spotify tính trên lượt nghe đủ 30 giây, khiến các nhà sản xuất rút ngắn phần intro và đẩy cao trào lên sớm.

4. **Phân cụm K-Means Machine Learning**:
   - Dữ liệu âm học được phân chia tự nhiên thành 4 cụm phong cách:
     - **Cụm 0 - High-Energy Party**: Danceability & Energy cao, âm lượng lớn.
     - **Cụm 1 - Acoustic & Chill**: Acousticness cao, năng lượng thấp, phù hợp thư giãn/học tập.
     - **Cụm 2 - Melancholic Mood**: Valence thấp, giai điệu da diết.
     - **Cụm 3 - Rap & Rhythmic Beat**: Speechiness cao, tempo biến hóa linh hoạt.
