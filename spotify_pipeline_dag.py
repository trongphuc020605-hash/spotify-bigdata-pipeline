"""
===================================================================================
ĐỒ ÁN TỐT NGHIỆP / HỌC PHẦN: BIG DATA
ĐỀ TÀI: PHÂN TÍCH XU HƯỚNG ÂM NHẠC TRÊN SPOTIFY (2017 - 2023)
TRƯỜNG ĐẠI HỌC CÔNG THƯƠNG TP.HCM (HUIT)
Nhóm sinh viên thực hiện:
- NGUYỄN HÀ TRỌNG PHÚC (Nhóm trưởng - MSSV: 2001230686)
- LÊ ĐÔNG TRIỀU
- NGUYỄN VŨ NGỌC NAM

FILE: spotify_pipeline_dag.py
MỤC ĐÍCH: GIẢI QUYẾT BƯỚC 9 (AIRFLOW DAG - TỰ ĐỘNG HÓA VÀ ĐIỀU PHỐI DATA PIPELINE)
===================================================================================
Chức năng điều phối của Airflow DAG:
1. Lên lịch thực thi định kỳ hàng ngày (schedule_interval='@daily').
2. Tự động hóa toàn bộ vòng đời xử lý dữ liệu Big Data (DataOps / Data Pipeline):
   - [Task 1]: Tiền xử lý, chuẩn hóa dữ liệu âm học và xuất Parquet (preprocess.py).
   - [Task 2]: Spark Submit thuật toán Machine Learning K-Means & RFM trên HDFS (pyspark_rfm_kmeans.py).
3. Thiết lập mối quan hệ phụ thuộc (Task Dependencies) nghiêm ngặt:
   - Task 2 CHỈ ĐƯỢC PHÉP CHẠY khi Task 1 hoàn thành thành công 100% (TriggerRule: ALL_SUCCESS).
4. Tương thích linh hoạt trên cả môi trường Lab Linux Cloud và máy tính cục bộ.
===================================================================================
"""

import os
from datetime import datetime, timedelta

# Import các thành phần cốt lõi của Apache Airflow
from airflow import DAG
from airflow.operators.bash import BashOperator

# Xử lý tương thích ngược cho DummyOperator (Airflow 1.x) và EmptyOperator (Airflow 2.x+)
try:
    from airflow.operators.empty import EmptyOperator
    DummyOrEmptyOperator = EmptyOperator
except ImportError:
    from airflow.operators.dummy import DummyOperator
    DummyOrEmptyOperator = DummyOperator


# =================================================================================
# 1. CẤU HÌNH THƯ MỤC LÀM VIỆC & THÔNG SỐ PIPELINE
# =================================================================================
# Đường dẫn thư mục dự án (Ưu tiên lấy biến môi trường, fallback thư mục lab Linux hoặc thư mục hiện tại)
DEFAULT_PROJECT_DIR = os.environ.get(
    "SPOTIFY_PROJECT_DIR",
    "/courses/2/projects/3/students/2001230686"  # Đường dẫn Workspace theo môi trường BigData Lab HUIT
)

# Đường dẫn HDFS mặc định cho pipeline
HDFS_INPUT_PARQUET = "hdfs://localhost:9000/spotify/processed/spotify_cleaned.parquet"
HDFS_OUTPUT_KMEANS = "hdfs://localhost:9000/spotify/output/kmeans_results.parquet"


# =================================================================================
# 2. ĐỊNH NGHĨA DEFAULT_ARGS (CÁC THAM SỐ MẶC ĐỊNH CHO TẤT CẢ TASKS)
# =================================================================================
# Các thông số này cực kỳ quan trọng khi báo cáo giảng viên:
# - owner: Tên sinh viên / nhóm kỹ sư phụ trách pipeline
default_args = {
    'owner': 'Nhom_Phuc_Trieu_Nam_HUIT',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}


# =================================================================================
# 3. KHỞI TẠO ĐỐI TƯỢNG DAG (DIRECTED ACYCLIC GRAPH)
# =================================================================================
with DAG(
    dag_id='spotify_bigdata_analytics_pipeline',
    default_args=default_args,
    description='Pipeline tự động hóa Tiền xử lý, Tính RFM & Phân cụm K-Means Spotify trên Hadoop/Spark',
    schedule_interval='@daily',           # Lên lịch chạy mỗi ngày một lần vào 00:00 UTC
    catchup=False,                         # Không chạy bù các chu kỳ trong quá khứ khi bật DAG lần đầu
    max_active_runs=1,                     # Chỉ cho phép tối đa 1 instance của pipeline chạy tại một thời điểm
    tags=['spotify', 'bigdata', 'hadoop', 'pyspark', 'rfm', 'kmeans', 'huit']
) as dag:

    # -----------------------------------------------------------------------------
    # Task 0: Đánh dấu bắt đầu Pipeline (Start Node)
    # -----------------------------------------------------------------------------
    start_pipeline = DummyOrEmptyOperator(
        task_id='start_pipeline'
    )

    # -----------------------------------------------------------------------------
    # Task 1: Tiền xử lý và làm sạch dữ liệu (Data Preprocessing & Cleaning)
    # Thực thi file preprocess.py để chuẩn hóa đặc trưng âm học, xử lý null và ghi parquet
    # -----------------------------------------------------------------------------
    task_preprocess_data = BashOperator(
        task_id='task_preprocess_data',
        bash_command="""
            echo "=================================================================="
            echo ">>> [TASK 1] BẮT ĐẦU QUÁ TRÌNH TIỀN XỬ LÝ DỮ LIỆU SPOTIFY..."
            echo ">>> Thư mục làm việc: {{ params.project_dir }}"
            echo "=================================================================="
            
            cd {{ params.project_dir }} && python3 preprocess.py
            
            if [ $? -eq 0 ]; then
                echo ">>> [TASK 1 THÀNH CÔNG] Dữ liệu đã được làm sạch và lưu trữ!"
            else
                echo ">>> [TASK 1 THẤT BẠI] Lỗi trong quá trình tiền xử lý!"
                exit 1
            fi
        """,
        params={'project_dir': DEFAULT_PROJECT_DIR}
    )

    # -----------------------------------------------------------------------------
    # Task 2: Chạy thuật toán Machine Learning K-Means & RFM trên Apache Spark
    # Sử dụng spark-submit để gửi tác vụ PySpark lên cụm phân tán
    # CHỈ CHẠY KHI TASK 1 THÀNH CÔNG
    # -----------------------------------------------------------------------------
    task_spark_rfm_kmeans = BashOperator(
        task_id='task_spark_rfm_kmeans',
        bash_command="""
            echo "=================================================================="
            echo ">>> [TASK 2] GỬI TÁC VỤ SPARK-SUBMIT THUẬT TOÁN K-MEANS & RFM..."
            echo ">>> Input HDFS: {{ params.input_hdfs }}"
            echo ">>> Output HDFS: {{ params.output_hdfs }}"
            echo "=================================================================="
            
            cd {{ params.project_dir }} && spark-submit \
                --master local[*] \
                --deploy-mode client \
                --driver-memory 4G \
                --executor-memory 4G \
                pyspark_rfm_kmeans.py \
                --input {{ params.input_hdfs }} \
                --output {{ params.output_hdfs }} \
                --k 4 \
                --seed 42
                
            if [ $? -eq 0 ]; then
                echo ">>> [TASK 2 THÀNH CÔNG] Thuật toán PySpark K-Means & RFM hoàn tất!"
            else
                echo ">>> [TASK 2 THẤT BẠI] Spark job gặp sự cố trong quá trình tính toán!"
                exit 1
            fi
        """,
        params={
            'project_dir': DEFAULT_PROJECT_DIR,
            'input_hdfs': HDFS_INPUT_PARQUET,
            'output_hdfs': HDFS_OUTPUT_KMEANS
        }
    )

    # -----------------------------------------------------------------------------
    # Task 3: Đánh dấu kết thúc Pipeline thành công (End Node)
    # -----------------------------------------------------------------------------
    end_pipeline = DummyOrEmptyOperator(
        task_id='end_pipeline'
    )

    # =============================================================================
    # THIẾT LẬP DÒNG DỮ LIỆU & QUAN HỆ PHỤ THUỘC (TASK DEPENDENCIES)
    # =============================================================================
    # Luồng tuần tự: Start -> Task 1 (Preprocess) -> Task 2 (Spark Submit) -> End
    # Task 2 sẽ chờ Task 1 thành công rực rỡ mới được phép kích hoạt
    start_pipeline >> task_preprocess_data >> task_spark_rfm_kmeans >> end_pipeline
