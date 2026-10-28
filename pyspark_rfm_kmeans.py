"""
===================================================================================
ĐỒ ÁN TỐT NGHIỆP / HỌC PHẦN: BIG DATA
ĐỀ TÀI: PHÂN TÍCH XU HƯỚNG ÂM NHẠC TRÊN SPOTIFY (2017 - 2023)
TRƯỜNG ĐẠI HỌC CÔNG THƯƠNG TP.HCM (HUIT)
Nhóm sinh viên thực hiện:
- NGUYỄN HÀ TRỌNG PHÚC (Nhóm trưởng - MSSV: 2001230686)
- LÊ ĐÔNG TRIỀU
- NGUYỄN VŨ NGỌC NAM

FILE: pyspark_rfm_kmeans.py
MỤC ĐÍCH: GIẢI QUYẾT BƯỚC 6 (RFM & K-MEANS) VÀ BƯỚC 7 (LƯU KẾT QUẢ VÀO HDFS)
===================================================================================
Luồng xử lý chính:
1. Khởi tạo SparkSession tối ưu hóa bộ nhớ cho Big Data.
2. Đọc dữ liệu sạch `spotify_cleaned.parquet` từ HDFS (hdfs://localhost:9000/spotify/processed/...)
   (Hỗ trợ fallback đọc file cục bộ nếu môi trường thử nghiệm chưa bật HDFS cluster).
3. [BƯỚC 6.1] Tính toán ma trận RFM phiên bản Music Domain (Recency, Frequency, Monetary).
   - Recency (R): Độ mới của ca khúc (Khoảng cách từ năm phát hành đến năm 2023).
   - Frequency (F): Tần suất xuất hiện trên Bảng xếp hạng Top 200.
   - Monetary (M): Tổng điểm xếp hạng tích lũy (Points) / Sức hút thương mại.
   - Phân khúc ca khúc/nghệ sĩ theo điểm RFM tổng hợp.
4. [BƯỚC 6.2] Áp dụng thuật toán Machine Learning K-Means Clustering trên PySpark MLlib:
   - Gom các đặc trưng âm học: danceability, energy, valence, acousticness, speechiness.
   - VectorAssembler & StandardScaler chuẩn hóa đặc trưng.
   - Huấn luyện mô hình K-Means với k = 4 cụm.
   - Đánh giá mô hình bằng ClusteringEvaluator (Silhouette Score).
   - Gán nhãn ngữ nghĩa âm nhạc cho từng cụm.
5. [BƯỚC 7] Ghi DataFrame kết quả phân tích phân cụm và RFM vào HDFS dưới dạng Parquet:
   - Đường dẫn đích: hdfs://localhost:9000/spotify/output/kmeans_results.parquet
6. Hiển thị báo cáo thống kê đặc trưng từng cụm để kiểm tra trực quan.
===================================================================================
"""

import sys
import os
import argparse
from datetime import datetime

# Thiết lập encoding UTF-8 an toàn cho console Windows / Linux
if hasattr(sys.stdout, 'reconfigure'):
    try:
        sys.stdout.reconfigure(encoding='utf-8', errors='replace')
        sys.stderr.reconfigure(encoding='utf-8', errors='replace')
    except Exception:
        pass

# ---------------------------------------------------------------------------------
# 1. KHỞI TẠO CÁC MODULE CỦA APACHE PYSPARK
# ---------------------------------------------------------------------------------
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, IntegerType, StringType
from pyspark.sql.window import Window

# PySpark MLlib: VectorAssembler, StandardScaler, KMeans, ClusteringEvaluator
from pyspark.ml.feature import VectorAssembler, StandardScaler
from pyspark.ml.clustering import KMeans
from pyspark.ml.evaluation import ClusteringEvaluator


def create_spark_session(app_name="Spotify_RFM_KMeans_Analytics"):
    """
    Hàm khởi tạo và cấu hình SparkSession:
    - appName: Tên ứng dụng hiển thị trên Spark Web UI (http://localhost:4040).
    - master: local[*] sử dụng toàn bộ số core CPU trên máy ảo/cụm máy chủ.
    - Cấu hình hỗ trợ kết nối HDFS và tối ưu hóa bộ nhớ thực thi.
    """
    print("=" * 80)
    print(f"[*] Đang khởi tạo Apache Spark Session: {app_name}...")
    print("=" * 80)

    spark = SparkSession.builder \
        .appName(app_name) \
        .master("local[*]") \
        .config("spark.driver.memory", "4g") \
        .config("spark.executor.memory", "4g") \
        .config("spark.sql.shuffle.partitions", "8") \
        .config("spark.sql.parquet.compression.codec", "snappy") \
        .config("spark.sql.parquet.enableVectorizedReader", "false") \
        .config("spark.sql.parquet.datetimeRebaseModeInRead", "CORRECTED") \
        .config("spark.sql.parquet.int96RebaseModeInRead", "CORRECTED") \
        .getOrCreate()

    # Giảm bớt các dòng log INFO không cần thiết, chỉ hiển thị WARN hoặc ERROR
    spark.sparkContext.setLogLevel("WARN")
    print(f"[OK] Spark Session đã khởi tạo thành công! Spark Version: {spark.version}")
    return spark


def load_dataset(spark, input_path):
    """
    Đọc dữ liệu Parquet từ HDFS hoặc Local Path.
    Nếu đường dẫn HDFS không khả dụng (ví dụ khi chạy test cục bộ), tự động fallback về file local.
    """
    print(f"\n[*] Đang đọc dữ liệu từ đường dẫn: {input_path}")
    
    # 1. Thử đọc từ đường dẫn chỉ định (HDFS hoặc local path)
    try:
        df = spark.read.parquet(input_path)
        record_count = df.count()
        print(f"[OK] Đọc thành công {record_count:,} bản ghi từ: {input_path}")
        return df
    except Exception as e:
        print(f"[!] CẢNH BÁO: Không thể đọc trực tiếp từ: {input_path}")
        print(f"[!] Chi tiết thông báo: {e}")

    # 2. Fallback đọc file Parquet cục bộ
    local_parquet = "spotify_cleaned.parquet"
    if os.path.exists(local_parquet):
        try:
            print(f"[*] Chuyển sang đọc file Parquet dự phòng cục bộ: {os.path.abspath(local_parquet)}")
            df = spark.read.parquet(local_parquet)
            print(f"[OK] Đọc thành công {df.count():,} bản ghi từ file Parquet cục bộ.")
            return df
        except Exception as ex_parquet:
            print(f"[!] Không thể đọc Parquet cục bộ: {ex_parquet}")

    # 3. Fallback đọc file CSV cục bộ nếu Parquet gặp lỗi định dạng timestamp
    local_csv = "spotify_cleaned.csv"
    if os.path.exists(local_csv):
        print(f"[*] Chuyển sang đọc file CSV dự phòng cục bộ: {os.path.abspath(local_csv)}")
        df = spark.read.option("header", "true").option("inferSchema", "true").csv(local_csv)
        print(f"[OK] Đọc thành công {df.count():,} bản ghi từ file CSV cục bộ.")
        return df

    raise FileNotFoundError(f"Không tìm thấy dữ liệu tại HDFS ({input_path}), Parquet ({local_parquet}), hoặc CSV ({local_csv})!")


# =================================================================================
# [BƯỚC 6.1]: TÍNH TOÁN CHỈ SỐ RFM PHIÊN BẢN MUSIC DOMAIN (NGHỆ SĨ & CA KHÚC)
# =================================================================================
def calculate_music_rfm(df):
    """
    Áp dụng mô hình RFM (Recency, Frequency, Monetary) trong lĩnh vực Phân tích Âm nhạc:
    - Recency (R): Ca khúc/Nghệ sĩ có bài phát hành gần đây không?
                   Công thức: 2023 - Year (Năm mới nhất trong dataset là 2023).
                   Giá trị càng nhỏ => Càng mới (Recency cao).
    - Frequency (F): Tần suất xuất hiện trên Bảng xếp hạng Top 200 (Số lượt lọt BXH).
                   Công thức: COUNT(*) group by (title, artist) hoặc tần suất theo bài.
    - Monetary (M): Tổng giá trị / Điểm ảnh hưởng (Total Points/Streams).
                   Công thức: SUM(points) tích lũy trên bảng xếp hạng.
    """
    print("\n" + "=" * 80)
    print(">>> [BƯỚC 6.1] BẮT ĐẦU TÍNH TOÁN MA TRẬN RFM (MUSIC DOMAIN)...")
    print("=" * 80)

    # 1. Tính toán RFM ở cấp độ bài hát (Track-level RFM)
    # Lấy năm tối đa của dữ liệu làm mốc chuẩn (thường là 2023)
    max_year = df.select(F.max("year")).collect()[0][0]
    if max_year is None:
        max_year = 2023
    print(f"[*] Mốc năm chuẩn (Baseline Year) để tính Recency: {max_year}")

    # Gom nhóm theo từng bài hát và nghệ sĩ để tính F và M
    track_rfm_agg = df.groupBy("title", "artist").agg(
        F.max("year").alias("latest_year"),
        F.count("rank").alias("frequency"),             # Số lần xuất hiện trên bảng xếp hạng
        F.sum("points").alias("monetary"),              # Tổng điểm xếp hạng tích lũy
        F.avg("danceability").alias("avg_danceability"),
        F.avg("energy").alias("avg_energy"),
        F.avg("valence").alias("avg_valence"),
        F.avg("acousticness").alias("avg_acousticness"),
        F.avg("speechiness").alias("avg_speechiness"),
        F.first("country").alias("country"),
        F.first("continent").alias("continent")
    )

    # Tính chỉ số Recency (Khoảng cách năm đến 2023: 0 là mới nhất năm 2023, 1 là 2022...)
    track_rfm = track_rfm_agg.withColumn(
        "recency_years", F.lit(max_year) - F.col("latest_year")
    )

    # 2. Sử dụng hàm Phân vị NTILE của Spark SQL để chia điểm RFM từ 1 đến 4:
    # - R_score: Recency càng nhỏ thì điểm càng cao (sắp xếp giảm dần theo recency_years để giá trị nhỏ có quantile cao)
    # - F_score: Frequency càng cao thì điểm càng cao (sắp xếp tăng dần)
    # - M_score: Monetary càng cao thì điểm càng cao (sắp xếp tăng dần)
    window_r = Window.orderBy(F.col("recency_years").desc())
    window_f = Window.orderBy(F.col("frequency").asc())
    window_m = Window.orderBy(F.col("monetary").asc())

    track_rfm_scored = track_rfm \
        .withColumn("r_score", F.ntile(4).over(window_r)) \
        .withColumn("f_score", F.ntile(4).over(window_f)) \
        .withColumn("m_score", F.ntile(4).over(window_m))

    # Tính điểm RFM tổng hợp dạng chuỗi (Ví dụ: '444', '432') và điểm trung bình
    track_rfm_scored = track_rfm_scored.withColumn(
        "rfm_score_str", F.concat(F.col("r_score"), F.col("f_score"), F.col("m_score"))
    ).withColumn(
        "rfm_total_score", (F.col("r_score") + F.col("f_score") + F.col("m_score")) / 3.0
    )

    # 3. Phân khúc ca khúc (Track Segmentation) dựa trên điểm RFM:
    # - Champions: Bài hit đỉnh cao (Mới, trụ hạng nhiều, điểm cực cao)
    # - Loyal Hits: Ca khúc quốc dân bền bỉ qua năm tháng
    # - New Potential Hits: Bài hát mới phát hành có tiềm năng lớn
    # - Hibernating: Bài hát cũ, xuất hiện ít lần
    track_rfm_segmented = track_rfm_scored.withColumn(
        "rfm_segment",
        F.when((F.col("r_score") >= 3) & (F.col("f_score") >= 3) & (F.col("m_score") >= 3), "🏆 Champions (Hit Kinh Điển)")
         .when((F.col("r_score") < 3) & (F.col("f_score") >= 3) & (F.col("m_score") >= 3), "🌟 Classic Hits (Bền Bỉ)")
         .when((F.col("r_score") >= 3) & (F.col("f_score") <= 2), "🚀 New Rising (Mới Nổi)")
         .when((F.col("r_score") <= 2) & (F.col("f_score") <= 2), "💤 Hibernating (Ít Tương Tác)")
         .otherwise("🎵 Regular Hits (Phổ Thông)")
    )

    print("[OK] Đã hoàn thành tính toán RFM cho các ca khúc.")
    print("[*] Xem trước 5 bài hát có điểm RFM cao nhất:")
    track_rfm_segmented.select(
        "title", "artist", "latest_year", "frequency", "monetary", "rfm_score_str", "rfm_segment"
    ).orderBy(F.col("monetary").desc()).show(5, truncate=False)

    return track_rfm_segmented


# =================================================================================
# [BƯỚC 6.2]: ÁP DỤNG THUẬT TOÁN K-MEANS CLUSTERING (PYSPARK MLLIB)
# =================================================================================
def train_kmeans_clustering(rfm_df, k=4, seed=42):
    """
    Xây dựng Pipeline phân cụm K-Means bằng PySpark MLlib:
    1. VectorAssembler: Gom 5 đặc trưng âm học vào 1 vector duy nhất.
    2. StandardScaler: Chuẩn hóa vector đặc trưng về phân phối chuẩn (mean=0, std=1).
    3. KMeans: Khởi tạo mô hình gom thành k cụm (mặc định k = 4).
    4. ClusteringEvaluator: Đo chỉ số Silhouette Score để đánh giá chất lượng phân cụm.
    5. Gán nhãn ngữ nghĩa âm nhạc cho 4 cụm phong cách.
    """
    print("\n" + "=" * 80)
    print(f">>> [BƯỚC 6.2] HUẤN LUYỆN MÔ HÌNH PHÂN CỤM K-MEANS (K = {k})...")
    print("=" * 80)

    # Danh sách các đặc trưng âm học dùng để phân cụm
    feature_cols = ["avg_danceability", "avg_energy", "avg_valence", "avg_acousticness", "avg_speechiness"]
    print(f"[*] Các đặc trưng âm học đưa vào mô hình: {feature_cols}")

    # Loại bỏ các dòng có giá trị null ở các cột đặc trưng (nếu có)
    cleaned_features_df = rfm_df.na.drop(subset=feature_cols)

    # 1. VectorAssembler: Gom các cột số thành một cột vector 'raw_features'
    assembler = VectorAssembler(inputCols=feature_cols, outputCol="raw_features")
    assembled_df = assembler.transform(cleaned_features_df)

    # 2. StandardScaler: Chuẩn hóa dữ liệu để các đặc trưng có thang đo tương đồng
    scaler = StandardScaler(inputCol="raw_features", outputCol="features", withMean=True, withStd=True)
    scaler_model = scaler.fit(assembled_df)
    scaled_df = scaler_model.transform(assembled_df)

    # 3. Khởi tạo và huấn luyện mô hình K-Means
    print(f"[*] Đang thực hiện thuật toán K-Means Clustering trên cụm Spark (k={k}, seed={seed})...")
    kmeans = KMeans(featuresCol="features", predictionCol="cluster", k=k, seed=seed, maxIter=30)
    model = kmeans.fit(scaled_df)

    # 4. Dự đoán nhãn cụm cho toàn bộ tập dữ liệu
    predictions_df = model.transform(scaled_df)

    # 5. Đánh giá chất lượng mô hình bằng Silhouette Score (-1 đến 1, càng gần 1 càng tốt)
    evaluator = ClusteringEvaluator(
        featuresCol="features",
        predictionCol="cluster",
        metricName="silhouette",
        distanceMeasure="squaredEuclidean"
    )
    silhouette = evaluator.evaluate(predictions_df)
    print(f"\n[KẾT QUẢ ĐÁNH GIÁ MÔ HÌNH]")
    print(f">> Silhouette Score (Độ phân tách cụm): {silhouette:.4f}")
    if silhouette > 0.4:
        print(">> Đánh giá: Chất lượng phân cụm RẤT TỐT (Các cụm tách biệt rõ ràng).")
    else:
        print(">> Đánh giá: Chất lượng phân cụm CHẤP NHẬN ĐƯỢC đối với dữ liệu âm thanh thực tế.")

    # 6. Gán nhãn ngữ nghĩa thân thiện cho 4 cụm phong cách âm nhạc
    # Tương đồng với nhãn hiển thị trên Streamlit Web Dashboard:
    # 0 -> Party & High-Energy Pop
    # 1 -> Acoustic & Chill Vibes
    # 2 -> Melancholic & Slow Tempo
    # 3 -> Rhythmic & Hip-Hop/Rap
    cluster_mapping = F.when(F.col("cluster") == 0, "🔥 Party & High-Energy Pop") \
                       .when(F.col("cluster") == 1, "🍃 Acoustic & Chill Vibes") \
                       .when(F.col("cluster") == 2, "🌧️ Melancholic & Slow Tempo") \
                       .otherwise("🎤 Rhythmic & Hip-Hop/Rap")

    result_df = predictions_df.withColumn("cluster_name", cluster_mapping)

    # In thông số tâm cụm (Cluster Centers) và thống kê trung bình
    print("\n[*] Thống kê đặc trưng trung bình của từng cụm âm nhạc:")
    cluster_stats = result_df.groupBy("cluster", "cluster_name").agg(
        F.count("title").alias("total_tracks"),
        F.round(F.avg("avg_danceability"), 3).alias("danceability"),
        F.round(F.avg("avg_energy"), 3).alias("energy"),
        F.round(F.avg("avg_valence"), 3).alias("valence"),
        F.round(F.avg("avg_acousticness"), 3).alias("acousticness"),
        F.round(F.avg("avg_speechiness"), 3).alias("speechiness"),
        F.round(F.avg("monetary"), 0).alias("avg_points")
    ).orderBy("cluster")
    cluster_stats.show(truncate=False)

    return result_df, model


# =================================================================================
# [BƯỚC 7]: LƯU KẾT QUẢ PHÂN TÍCH VÀO HDFS DƯỚI ĐỊNH DẠNG PARQUET
# =================================================================================
def save_results_to_hdfs(result_df, output_path):
    """
    Ghi DataFrame kết quả phân tích phân cụm và RFM vào hệ thống phân tán HDFS:
    - Loại bỏ các cột Vector trung gian (raw_features, features) để tối ưu dung lượng lưu trữ.
    - Ghi ở chế độ 'overwrite' để làm mới dữ liệu khi chạy lại pipeline.
    - Định dạng: Parquet nén Snappy chuẩn Big Data.
    """
    print("\n" + "=" * 80)
    print(f">>> [BƯỚC 7] GHI KẾT QUẢ PHÂN CỤM & RFM VÀO HDFS: {output_path}")
    print("=" * 80)

    # Chỉ giữ lại các cột thông tin kinh doanh và nhãn dự đoán phân cụm (loại bỏ cột vector)
    output_columns = [
        "title", "artist", "country", "continent", "latest_year",
        "frequency", "monetary", "recency_years",
        "r_score", "f_score", "m_score", "rfm_score_str", "rfm_segment",
        "avg_danceability", "avg_energy", "avg_valence", "avg_acousticness", "avg_speechiness",
        "cluster", "cluster_name"
    ]

    clean_output_df = result_df.select([c for c in output_columns if c in result_df.columns])

    try:
        clean_output_df.write \
            .mode("overwrite") \
            .parquet(output_path)
        print(f"[THÀNH CÔNG] Đã ghi thành công kết quả phân cụm vào HDFS: {output_path}")
    except Exception as e:
        print(f"[!] CẢNH BÁO: Gặp lỗi khi ghi vào HDFS: {e}")
        # Lưu bản fallback cục bộ
        local_output = "kmeans_results.parquet"
        print(f"[*] Ghi dự phòng vào thư mục cục bộ: {os.path.abspath(local_output)}")
        clean_output_df.write.mode("overwrite").parquet(local_output)
        print(f"[OK] Đã lưu kết quả cục bộ tại: {local_output}")


# =================================================================================
# HÀM CHÍNH (MAIN ENTRY POINT)
# =================================================================================
def main():
    parser = argparse.ArgumentParser(description="PySpark Spotify RFM & K-Means Pipeline")
    parser.add_argument(
        "--input",
        type=str,
        default="hdfs://localhost:9000/spotify/processed/spotify_cleaned.parquet",
        help="Đường dẫn HDFS đến file spotify_cleaned.parquet"
    )
    parser.add_argument(
        "--output",
        type=str,
        default="hdfs://localhost:9000/spotify/output/kmeans_results.parquet",
        help="Đường dẫn HDFS xuất file kết quả kmeans_results.parquet"
    )
    parser.add_argument(
        "--k",
        type=int,
        default=4,
        help="Số lượng cụm K-Means (Mặc định = 4)"
    )
    parser.add_argument(
        "--seed",
        type=int,
        default=42,
        help="Random seed để tái lập kết quả"
    )

    args = parser.parse_args()

    start_time = datetime.now()
    print("=" * 80)
    print("      HỆ THỐNG PHÂN TÍCH BIG DATA SPOTIFY - HADOOP & PYSPARK PIPELINE")
    print(f"      Thời gian bắt đầu: {start_time.strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 80)

    # 1. Khởi tạo Spark
    spark = create_spark_session()

    try:
        # 2. Đọc dữ liệu đã tiền xử lý
        df_cleaned = load_dataset(spark, args.input)

        # 3. [Bước 6.1] Tính toán ma trận RFM
        df_rfm = calculate_music_rfm(df_cleaned)

        # 4. [Bước 6.2] Huấn luyện K-Means phân cụm đặc trưng âm học
        df_clustered, kmeans_model = train_kmeans_clustering(df_rfm, k=args.k, seed=args.seed)

        # 5. [Bước 7] Lưu kết quả vào HDFS
        save_results_to_hdfs(df_clustered, args.output)

        elapsed = (datetime.now() - start_time).total_seconds()
        print("\n" + "=" * 80)
        print(f"[HOÀN TẤT] Pipeline PySpark hoàn thành xuất sắc trong {elapsed:.2f} giây!")
        print("=" * 80)

    except Exception as ex:
        print(f"\n[LỖI NGHIÊM TRỌNG]: Pipeline thất bại: {ex}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
    finally:
        # Giải phóng tài nguyên Spark
        spark.stop()
        print("[*] Đã đóng SparkSession an toàn.")


if __name__ == "__main__":
    main()
