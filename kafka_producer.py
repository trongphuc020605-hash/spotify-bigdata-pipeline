"""
===================================================================================
ĐỒ ÁN TỐT NGHIỆP / HỌC PHẦN: BIG DATA
ĐỀ TÀI: PHÂN TÍCH XU HƯỚNG ÂM NHẠC TRÊN SPOTIFY (2017 - 2023)
TRƯỜNG ĐẠI HỌC CÔNG THƯƠNG TP.HCM (HUIT)
Nhóm sinh viên thực hiện:
- NGUYỄN HÀ TRỌNG PHÚC (Nhóm trưởng - MSSV: 2001230686)
- LÊ ĐÔNG TRIỀU
- NGUYỄN VŨ NGỌC NAM

FILE: kafka_producer.py
MỤC ĐÍCH: GIẢI QUYẾT BƯỚC 8 (KAFKA INGEST - REAL-TIME STREAMING SIMULATION)
===================================================================================
Chức năng:
1. Kết nối tới Apache Kafka Broker (mặc định: localhost:9092).
2. Tự động kiểm tra hoặc cấu hình Topic đích: 'spotify_streaming'.
3. Đọc dữ liệu từng dòng từ file cục bộ `spotify_top200_sample.csv` (tự động phát hiện delimiter ';' hoặc ',').
4. Chuẩn hóa bản ghi thành payload JSON có cấu trúc chuẩn streaming (kèm event_id, timestamp).
5. Gửi (Produce) message vào Kafka Topic bất đồng bộ (Asynchronous) kèm Callback xác nhận.
6. Mô phỏng luồng phát trực tiếp (Streaming) với độ trễ 1 - 2 giây mỗi bài hát.
7. Xử lý đóng kết nối an toàn (Graceful Shutdown) khi người dùng nhấn Ctrl+C.
===================================================================================
"""

import sys
import os
import csv
import json
import time
import random
import uuid
import argparse
from datetime import datetime

# Thiết lập encoding UTF-8 an toàn cho console Windows / Linux
if hasattr(sys.stdout, 'reconfigure'):
    try:
        sys.stdout.reconfigure(encoding='utf-8', errors='replace')
        sys.stderr.reconfigure(encoding='utf-8', errors='replace')
    except Exception:
        pass

# Import thư viện Kafka Producer (Ưu tiên kafka-python, fallback confluent_kafka)
try:
    from kafka import KafkaProducer
    from kafka.errors import KafkaError
    KAFKA_LIB = "kafka-python"
except ImportError:
    try:
        from confluent_kafka import Producer as ConfluentProducer
        KAFKA_LIB = "confluent-kafka"
    except ImportError:
        KAFKA_LIB = None


def detect_delimiter(file_path):
    """Tự động phát hiện dấu phân cách ; hoặc , của file CSV"""
    with open(file_path, 'r', encoding='utf-8', errors='ignore') as f:
        first_line = f.readline()
        if first_line.count(';') > first_line.count(','):
            return ';'
    return ','


def build_kafka_producer(bootstrap_servers):
    """
    Khởi tạo Kafka Producer với cấu hình tối ưu hóa cho Streaming:
    - bootstrap_servers: Địa chỉ Kafka Broker (localhost:9092)
    - value_serializer: Tự động chuyển đổi Python Dict thành byte chuỗi JSON (UTF-8)
    - acks: 'all' hoặc 1 để đảm bảo broker đã nhận được message an toàn
    - retries: Số lần thử lại nếu gặp sự cố mạng tạm thời
    """
    if KAFKA_LIB is None:
        raise ImportError(
            "Chưa cài đặt thư viện Kafka! Vui lòng chạy lệnh: pip install kafka-python "
            "hoặc pip install confluent-kafka"
        )

    print(f"[*] Đang khởi tạo kết nối tới Kafka Broker: {bootstrap_servers} (Thư viện: {KAFKA_LIB})...")
    
    try:
        if KAFKA_LIB == "kafka-python":
            producer = KafkaProducer(
                bootstrap_servers=bootstrap_servers.split(','),
                value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode('utf-8'),
                key_serializer=lambda k: k.encode('utf-8') if k else None,
                acks=1,
                retries=3,
                request_timeout_ms=3000,
                linger_ms=10
            )
            print("[OK] Kết nối Kafka Producer (kafka-python) thành công!")
            return producer
        else:
            # confluent-kafka
            conf = {
                'bootstrap.servers': bootstrap_servers,
                'client.id': 'spotify-streaming-producer',
                'acks': '1'
            }
            producer = ConfluentProducer(conf)
            print("[OK] Kết nối Kafka Producer (confluent-kafka) thành công!")
            return producer
    except Exception as e:
        print(f"\n[!] THÔNG BÁO: Chưa phát hiện Kafka Broker hoạt động tại {bootstrap_servers}.")
        print(f"[*] Gợi ý: Trên Lab Linux trường, Kafka Broker mặc định đã chạy.")
        print(f"[*] Trên máy cá nhân: Chuyển sang CHẾ ĐỘ MÔ PHỎNG STREAMING (Mock Live Streaming Ingestion)")
        print(f"[*] Hệ thống vẫn parse JSON và mô phỏng luồng phát trực tiếp ca khúc realtime để demo báo cáo!\n")
        return None


def on_send_success(record_metadata):
    """Callback xử lý khi gửi message thành công (dành cho kafka-python)"""
    print(f"   └── [ACK] Topic: {record_metadata.topic} | Partition: {record_metadata.partition} | Offset: {record_metadata.offset}")


def on_send_error(excp):
    """Callback xử lý khi gặp lỗi gửi message"""
    print(f"   └── [LỖI KAFKA] Không thể gửi message: {excp}", file=sys.stderr)


def clean_row_to_json(row):
    """
    Chuyển đổi một dòng dữ liệu thô từ CSV sang đối tượng JSON chuẩn hóa:
    - Bổ sung trường định danh duy nhất (event_id)
    - Bổ sung thời gian gửi streaming thực tế (stream_timestamp)
    - Ép kiểu dữ liệu số (float, int) chuẩn xác
    """
    def to_float(val, default=0.0):
        try:
            return float(val) if val is not None and str(val).strip() != "" else default
        except (ValueError, TypeError):
            return default

    def to_int(val, default=0):
        try:
            return int(float(val)) if val is not None and str(val).strip() != "" else default
        except (ValueError, TypeError):
            return default

    # Tìm các trường dựa trên key trong file CSV
    rank = to_int(row.get('Song Rank') or row.get('rank') or row.get('ranking'), default=100)
    title = str(row.get('Title of the Song') or row.get('title') or row.get('track_name') or 'Unknown Track').strip()
    artist = str(row.get('Artist Name') or row.get('artist') or 'Unknown Artist').strip()
    date_str = str(row.get('Date') or row.get('date') or datetime.now().strftime('%Y-%m-%d')).strip()
    
    danceability = to_float(row.get('Danceability') or row.get('danceability'), default=0.65)
    energy = to_float(row.get('Energy') or row.get('energy'), default=0.65)
    valence = to_float(row.get('Valence') or row.get('valence'), default=0.50)
    acousticness = to_float(row.get('Acousticness') or row.get('acousticness'), default=0.20)
    speechiness = to_float(row.get('Speechiness') or row.get('speechiness'), default=0.08)
    loudness = to_float(row.get('Loudness') or row.get('loudness'), default=-6.0)
    tempo = to_float(row.get('Tempo') or row.get('tempo'), default=120.0)
    points = to_int(row.get('Points (Total)') or row.get('points'), default=100)
    
    country = str(row.get('Nationality of each artist') or row.get('country') or 'Global').strip()
    continent = str(row.get("Artist's Continent") or row.get('continent') or 'Global').strip()
    url = str(row.get('URL Link to the Song') or row.get('url') or '').strip()

    payload = {
        "event_id": str(uuid.uuid4()),
        "stream_timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "rank": rank,
        "title": title,
        "artist": artist,
        "date": date_str,
        "danceability": danceability,
        "energy": energy,
        "valence": valence,
        "acousticness": acousticness,
        "speechiness": speechiness,
        "loudness": loudness,
        "tempo": tempo,
        "points": points,
        "country": country,
        "continent": continent,
        "url": url
    }
    return payload


def stream_dataset_to_kafka(file_path, producer, topic, min_delay=1.0, max_delay=2.0, max_records=None):
    """
    Đọc tệp dữ liệu mẫu và đẩy từng dòng vào Kafka Topic:
    - min_delay, max_delay: Khoảng thời gian nghỉ giữa mỗi lần gửi (mặc định 1.0 - 2.0s).
    - max_records: Giới hạn số bản ghi gửi (None = gửi đến hết file hoặc lặp lại).
    """
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"Không tìm thấy file dữ liệu mẫu tại: {file_path}")

    delimiter = detect_delimiter(file_path)
    print("=" * 80)
    print(f"[*] Bắt đầu đọc file: {file_path}")
    print(f"[*] Dấu phân cách phát hiện: '{delimiter}'")
    print(f"[*] Topic Kafka mục tiêu: '{topic}'")
    print(f"[*] Tần suất gửi (Interval): {min_delay:.1f}s - {max_delay:.1f}s / bản ghi")
    print("=" * 80)
    print(">> Nhấn tổ hợp phím [Ctrl + C] bất kỳ lúc nào để dừng luồng Streaming <<\n")

    sent_count = 0

    with open(file_path, 'r', encoding='utf-8', errors='ignore') as csvfile:
        reader = csv.DictReader(csvfile, delimiter=delimiter)
        
        for row in reader:
            payload = clean_row_to_json(row)
            track_key = f"{payload['artist']}_{payload['title']}"

            # Hiển thị thông tin bài hát đang gửi
            sent_count += 1
            print(f"[{sent_count:04d} | {payload['stream_timestamp']}] Gửi ca khúc: "
                  f"#{payload['rank']:03d} | '{payload['title']}' - {payload['artist']} "
                  f"[Dance: {payload['danceability']:.2f}, Energy: {payload['energy']:.2f}, Points: {payload['points']}]")

            # Gửi dữ liệu vào Kafka Topic (hoặc mô phỏng gửi nếu broker offline)
            if producer is not None:
                if KAFKA_LIB == "kafka-python":
                    future = producer.send(topic, key=track_key, value=payload)
                    future.add_callback(on_send_success)
                    future.add_errback(on_send_error)
                else:
                    # confluent-kafka
                    producer.produce(
                        topic,
                        key=track_key,
                        value=json.dumps(payload, ensure_ascii=False).encode('utf-8'),
                        callback=lambda err, msg: print(f"   └── [ACK confluent] {msg.topic()} [{msg.partition()}] offset {msg.offset()}") if not err else print(f"   └── [ERR] {err}")
                    )
                    producer.poll(0)
            else:
                # Mock Mode (Hữu ích khi demo trên máy cá nhân chưa cài/bật Kafka Broker)
                print(f"   └── [MOCK-INGEST] -> Topic: {topic} | EventID: {payload['event_id'][:8]}... | JSON Payload Serialized OK")

            # Cập nhật buffer thời gian thực cho Web Dashboard Streamlit
            try:
                buffer_file = "kafka_stream_buffer.json"
                existing_buffer = []
                if os.path.exists(buffer_file):
                    try:
                        with open(buffer_file, 'r', encoding='utf-8') as bf:
                            bdata = json.load(bf)
                            existing_buffer = bdata.get("events", [])
                    except Exception:
                        existing_buffer = []
                existing_buffer.insert(0, payload)
                existing_buffer = existing_buffer[:25] # Giữ 25 sự kiện mới nhất
                with open(buffer_file, 'w', encoding='utf-8') as bf:
                    json.dump({
                        "total_sent": sent_count,
                        "last_updated": payload['stream_timestamp'],
                        "events": existing_buffer
                    }, bf, ensure_ascii=False, indent=2)
            except Exception:
                pass

            # Giới hạn số lượng bản ghi nếu có cấu hình
            if max_records and sent_count >= max_records:
                print(f"\n[*] Đã gửi đủ số bản ghi giới hạn: {max_records}. Hoàn tất!")
                break

            # Tạo độ trễ ngẫu nhiên 1 - 2 giây để mô phỏng luồng người dùng nghe nhạc realtime
            sleep_duration = random.uniform(min_delay, max_delay)
            time.sleep(sleep_duration)


def main():
    parser = argparse.ArgumentParser(description="Spotify Real-Time Streaming Producer (Kafka Ingest)")
    parser.add_argument(
        "--bootstrap-servers",
        type=str,
        default="localhost:9092",
        help="Địa chỉ Kafka Broker (Mặc định: localhost:9092)"
    )
    parser.add_argument(
        "--topic",
        type=str,
        default="spotify_streaming",
        help="Tên topic Kafka cần gửi dữ liệu vào (Mặc định: spotify_streaming)"
    )
    parser.add_argument(
        "--file",
        type=str,
        default="spotify_top200_sample.csv",
        help="Đường dẫn file CSV mẫu để gửi dữ liệu (Mặc định: spotify_top200_sample.csv)"
    )
    parser.add_argument(
        "--min-delay",
        type=float,
        default=1.0,
        help="Thời gian chờ tối thiểu giữa các message (giây)"
    )
    parser.add_argument(
        "--max-delay",
        type=float,
        default=2.0,
        help="Thời gian chờ tối đa giữa các message (giây)"
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Giới hạn số bản ghi gửi (Mặc định: None - gửi hết file)"
    )

    args = parser.parse_args()

    producer = None
    try:
        producer = build_kafka_producer(args.bootstrap_servers)
        stream_dataset_to_kafka(
            file_path=args.file,
            producer=producer,
            topic=args.topic,
            min_delay=args.min_delay,
            max_delay=args.max_delay,
            max_records=args.limit
        )
    except KeyboardInterrupt:
        print("\n\n[*] Người dùng đã chủ động dừng chương trình (Ctrl + C).")
    except Exception as e:
        print(f"\n[!] LỖI THỰC THI: {e}", file=sys.stderr)
        import traceback
        traceback.print_exc()
    finally:
        if producer:
            print("[*] Đang làm rỗng bộ đệm (flushing) và đóng kết nối Kafka Producer...")
            if KAFKA_LIB == "kafka-python":
                producer.flush()
                producer.close()
            else:
                producer.flush(timeout=5)
            print("[OK] Đã đóng Kafka Producer an toàn!")


if __name__ == "__main__":
    main()
