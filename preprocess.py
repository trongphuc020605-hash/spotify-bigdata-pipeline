"""
Module tiền xử lý dữ liệu Big Data cho đồ án Spotify (2017 - 2023)
TRƯỜNG ĐẠI HỌC CÔNG THƯƠNG TP.HCM (HUIT)
Nhóm sinh viên thực hiện:
- NGUYỄN HÀ TRỌNG PHÚC (Nhóm trưởng - MSSV: 2001230686)
- LÊ ĐÔNG TRIỀU
- NGUYỄN VŨ NGỌC NAM
- Hỗ trợ tự động nhận diện delimiter (';' hoặc ',')
- Chuẩn hóa tên cột thông minh (Fuzzy header matching)
- Xử lý Missing values, chuẩn hóa kiểu dữ liệu
- Trích xuất đặc trưng thời gian (Year, Month, Year-Month, Quý)
- Tính toán ma trận tổng hợp và xuất tệp JSON/Parquet phục vụ Dashboard siêu tốc
"""

import os
import json
import numpy as np
import pandas as pd
from datetime import datetime

def detect_delimiter(file_path):
    """Tự động phát hiện dấu phân cách ; hoặc ,"""
    with open(file_path, 'r', encoding='utf-8', errors='ignore') as f:
        first_line = f.readline()
        if first_line.count(';') > first_line.count(','):
            return ';'
    return ','

def find_col(columns, candidates):
    """Tìm tên cột phù hợp nhất trong danh sách các từ khóa"""
    cols_lower = {str(c).strip().lower(): c for c in columns}
    for cand in candidates:
        cand_lower = cand.lower()
        if cand_lower in cols_lower:
            return cols_lower[cand_lower]
        for col_l, original_col in cols_lower.items():
            if cand_lower in col_l:
                return original_col
    return None

def load_and_clean_data(file_path):
    """Đọc và chuẩn hóa dữ liệu Spotify"""
    print(f"[*] Dang tai du lieu tu: {file_path}")
    delimiter = detect_delimiter(file_path)
    print(f"[*] Phat hien dau phan cach: '{delimiter}'")
    
    df = pd.read_csv(file_path, sep=delimiter, low_memory=False, on_bad_lines='skip')
    print(f"[*] Tong so dong ban dau: {len(df):,}")

    cols = df.columns.tolist()
    
    col_rank = find_col(cols, ['Song Rank', 'rank', 'ranking', 'position'])
    col_title = find_col(cols, ['Title of the Song', 'track_name', 'title', 'song_name', 'name'])
    col_artist = find_col(cols, ['Artist Name', 'artist_name', 'artist', 'artists'])
    col_date = find_col(cols, ['Date', 'release_date', 'date', 'publish_date'])
    col_dance = find_col(cols, ['Danceability', 'danceability', 'dance'])
    col_energy = find_col(cols, ['Energy', 'energy'])
    col_loud = find_col(cols, ['Loudness', 'loudness'])
    col_speech = find_col(cols, ['Speechiness', 'speechiness'])
    col_acoustic = find_col(cols, ['Acousticness', 'acousticness'])
    col_instru = find_col(cols, ['Instrumentalness', 'instrumentalness'])
    col_valence = find_col(cols, ['Valence', 'valence', 'mood'])
    col_tempo = find_col(cols, ['Tempo', 'tempo', 'bpm'])
    col_dur = find_col(cols, ['Duration (ms)', 'duration_ms', 'duration', 'duration_minutes'])
    col_points = find_col(cols, ['Points (Total)', 'points', 'stream_count', 'streams', 'popularity'])
    col_country = find_col(cols, ['Nationality of each artist', 'nationality', 'country'])
    col_continent = find_col(cols, ["Artist's Continent", 'continent', 'region'])
    col_url = find_col(cols, ['URL Link to the Song', 'song_url', 'url'])

    # Tạo DataFrame chuẩn hóa
    clean_df = pd.DataFrame()
    clean_df['title'] = df[col_title].astype(str) if col_title else "Unknown Track"
    clean_df['artist'] = df[col_artist].astype(str) if col_artist else "Unknown Artist"
    
    # Xử lý ngày tháng
    if col_date:
        clean_df['date'] = pd.to_datetime(df[col_date], errors='coerce')
        clean_df['year'] = clean_df['date'].dt.year
        clean_df['month'] = clean_df['date'].dt.month
        clean_df['year_month'] = clean_df['date'].dt.strftime('%Y-%m')
    else:
        clean_df['date'] = pd.NaT
        clean_df['year'] = 2020
        clean_df['month'] = 1
        clean_df['year_month'] = '2020-01'

    # Filter years 2017 to 2023 if available
    clean_df['year'] = pd.to_numeric(clean_df['year'], errors='coerce')
    clean_df = clean_df.dropna(subset=['year'])
    clean_df['year'] = clean_df['year'].astype(int)

    # Audio features
    feature_map = {
        'danceability': col_dance,
        'energy': col_energy,
        'loudness': col_loud,
        'speechiness': col_speech,
        'acousticness': col_acoustic,
        'instrumentalness': col_instru,
        'valence': col_valence,
        'tempo': col_tempo
    }

    for standard_col, orig_col in feature_map.items():
        if orig_col and orig_col in df:
            clean_df[standard_col] = pd.to_numeric(df[orig_col], errors='coerce')
            # Normalize percentage (0-100) to (0-1) if needed
            if standard_col not in ['loudness', 'tempo']:
                if clean_df[standard_col].max() > 1.5:
                    clean_df[standard_col] = clean_df[standard_col] / 100.0
        else:
            clean_df[standard_col] = np.nan

    # Fill defaults for audio features if missing
    clean_df['danceability'] = clean_df['danceability'].fillna(0.65)
    clean_df['energy'] = clean_df['energy'].fillna(0.65)
    clean_df['valence'] = clean_df['valence'].fillna(0.50)
    clean_df['acousticness'] = clean_df['acousticness'].fillna(0.20)
    clean_df['speechiness'] = clean_df['speechiness'].fillna(0.08)
    clean_df['instrumentalness'] = clean_df['instrumentalness'].fillna(0.01)
    clean_df['loudness'] = clean_df['loudness'].fillna(-6.0)
    clean_df['tempo'] = clean_df['tempo'].fillna(120.0)

    # Rank & Points
    if col_rank:
        clean_df['rank'] = pd.to_numeric(df[col_rank], errors='coerce').fillna(100).astype(int)
    else:
        clean_df['rank'] = 100

    if col_points:
        clean_df['points'] = pd.to_numeric(df[col_points], errors='coerce').fillna(201 - clean_df['rank'])
    else:
        clean_df['points'] = 201 - clean_df['rank']

    # Duration
    if col_dur:
        dur_val = pd.to_numeric(df[col_dur], errors='coerce').fillna(180000)
        # Check if duration is in minutes or ms
        if dur_val.mean() < 30: # In minutes
            clean_df['duration_ms'] = (dur_val * 60 * 1000).astype(int)
            clean_df['duration_min'] = dur_val
        else:
            clean_df['duration_ms'] = dur_val.astype(int)
            clean_df['duration_min'] = (dur_val / 60000).round(2)
    else:
        clean_df['duration_ms'] = 180000
        clean_df['duration_min'] = 3.0

    # Nationality & Continent
    clean_df['country'] = df[col_country].astype(str).fillna("Unknown") if col_country else "Global"
    clean_df['continent'] = df[col_continent].astype(str).fillna("Global") if col_continent else "Global"
    clean_df['url'] = df[col_url].astype(str) if col_url else ""

    # Sort and reset index
    clean_df = clean_df.sort_values(by=['year', 'date'], ascending=[True, True]).reset_index(drop=True)
    print(f"[OK] Du lieu sau khi lam sach: {len(clean_df):,} dong.")
    return clean_df

def generate_summary_json(df, output_json_path="dashboard_summary.json"):
    """Tạo file JSON chứa thông tin thống kê tổng hợp phục vụ giao diện Web HTML"""
    # Filter 2017-2023
    f_df = df[(df['year'] >= 2017) & (df['year'] <= 2023)]
    if f_df.empty:
        f_df = df

    # Yearly trends
    yearly = f_df.groupby('year').agg({
        'danceability': 'mean',
        'energy': 'mean',
        'valence': 'mean',
        'acousticness': 'mean',
        'speechiness': 'mean',
        'duration_min': 'mean',
        'points': 'sum',
        'title': 'count'
    }).reset_index()

    # Top artists
    top_artists = f_df.groupby('artist').agg({
        'points': 'sum',
        'title': 'count'
    }).sort_values(by='points', ascending=False).head(15).reset_index()

    # Top songs
    top_songs = f_df.groupby(['title', 'artist']).agg({
        'points': 'sum',
        'rank': 'min',
        'danceability': 'first',
        'energy': 'first',
        'valence': 'first'
    }).sort_values(by='points', ascending=False).head(15).reset_index()

    # Continents / Nationalities
    top_countries = f_df['country'].value_counts().head(10).to_dict()
    continent_counts = f_df['continent'].value_counts().to_dict()

    summary = {
        "metadata": {
            "total_records": int(len(f_df)),
            "unique_artists": int(f_df['artist'].nunique()),
            "unique_tracks": int(f_df['title'].nunique()),
            "years": [int(y) for y in yearly['year'].tolist()],
            "avg_danceability": round(float(f_df['danceability'].mean()) * 100, 1),
            "avg_energy": round(float(f_df['energy'].mean()) * 100, 1),
            "avg_valence": round(float(f_df['valence'].mean()) * 100, 1),
            "avg_acousticness": round(float(f_df['acousticness'].mean()) * 100, 1)
        },
        "trends": {
            "years": [str(y) for y in yearly['year']],
            "danceability": [round(float(v) * 100, 2) for v in yearly['danceability']],
            "energy": [round(float(v) * 100, 2) for v in yearly['energy']],
            "valence": [round(float(v) * 100, 2) for v in yearly['valence']],
            "acousticness": [round(float(v) * 100, 2) for v in yearly['acousticness']],
            "duration_min": [round(float(v), 2) for v in yearly['duration_min']]
        },
        "top_artists": {
            "names": top_artists['artist'].tolist(),
            "points": [int(p) for p in top_artists['points']],
            "song_counts": [int(c) for c in top_artists['title']]
        },
        "top_songs": [
            {
                "title": row['title'],
                "artist": row['artist'],
                "points": int(row['points']),
                "best_rank": int(row['rank'])
            }
            for _, row in top_songs.iterrows()
        ],
        "countries": top_countries,
        "continents": continent_counts
    }

    with open(output_json_path, 'w', encoding='utf-8') as f:
        json.dump(summary, f, ensure_ascii=False, indent=2)
    
    print(f"[OK] Da xuat tom tat du lieu toi: {output_json_path}")
    return summary

if __name__ == "__main__":
    sample_file = "spotify_top200_sample.csv"
    if os.path.exists(sample_file):
        df_cleaned = load_and_clean_data(sample_file)
        generate_summary_json(df_cleaned)
        # Lưu bản Parquet và CSV đã làm sạch (Tương thích 100% với PySpark & HDFS)
        try:
            # Ép kiểu date sang datetime64[ms] để tránh lỗi INT64 TIMESTAMP(NANOS) trên Spark
            df_export = df_cleaned.copy()
            if 'date' in df_export.columns and pd.api.types.is_datetime64_any_dtype(df_export['date']):
                df_export['date'] = df_export['date'].astype('datetime64[ms]')
            df_export.to_parquet("spotify_cleaned.parquet", index=False, coerce_timestamps='ms')
            print("[OK] Da luu spotify_cleaned.parquet (Chuan PySpark datetime64[ms])")
        except Exception as e:
            print(f"[!] Khong the luu parquet: {e}")
        df_cleaned.to_csv("spotify_cleaned.csv", index=False, encoding="utf-8")
        print("[OK] Da luu spotify_cleaned.csv")
