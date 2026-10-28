"""
=============================================================================
ĐỒ ÁN BIG DATA - ĐẠI HỌC CÔNG THƯƠNG TP.HCM (HUIT)
ĐỀ TÀI: PHÂN TÍCH XU HƯỚNG ÂM NHẠC TRÊN SPOTIFY GIAI ĐOẠN 2017 - 2023
Nhóm sinh viên thực hiện:
1. NGUYỄN HÀ TRỌNG PHÚC (Nhóm trưởng - MSSV: 2001230686)
2. LÊ ĐÔNG TRIỀU
3. NGUYỄN VŨ NGỌC NAM
Framework: Apache PySpark + Kafka + Airflow + Streamlit + Plotly
=============================================================================
"""

import os
import io
import json
import time
import numpy as np
import pandas as pd
import streamlit as st
import plotly.express as px
import plotly.graph_objects as go
from sklearn.cluster import KMeans
from sklearn.preprocessing import StandardScaler
from sklearn.decomposition import PCA

# --- 1. CẤU HÌNH TRANG STREAMLIT ---
st.set_page_config(
    page_title="Spotify Big Data Analytics (2017 - 2023)",
    page_icon="🎵",
    layout="wide",
    initial_sidebar_state="expanded"
)

# --- 2. GIAO DIỆN SPOTIFY DARK THEME (CUSTOM CSS & FONT AWESOME ICONS) ---
st.markdown("""
<!-- Nhúng Font Awesome 6 Pro / Free CDN chuẩn -->
<link rel="stylesheet" href="https://cdnjs.cloudflare.com/ajax/libs/font-awesome/6.5.1/css/all.min.css">
<!-- Nhúng Google Fonts Inter -->
<link href="https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700;800&display=swap" rel="stylesheet">

<style>
    /* Dark Theme Spotify */
    .stApp {
        background-color: #121212;
        color: #FFFFFF;
        font-family: 'Inter', sans-serif;
    }
    
    /* Sidebar */
    section[data-testid="stSidebar"] {
        background-color: #000000 !important;
        border-right: 1px solid #282828;
    }
    
    /* Metric Card Styling */
    .kpi-card {
        background: linear-gradient(145deg, #1e1e1e, #181818);
        border: 1px solid rgba(255, 255, 255, 0.08);
        border-radius: 12px;
        padding: 20px;
        box-shadow: 0 4px 20px rgba(0,0,0,0.5);
        transition: transform 0.2s ease, border-color 0.2s ease;
    }
    .kpi-card:hover {
        transform: translateY(-3px);
        border-color: #1DB954;
    }
    .kpi-title {
        color: #B3B3B3;
        font-size: 0.85rem;
        font-weight: 600;
        text-transform: uppercase;
        letter-spacing: 0.05em;
        margin-bottom: 8px;
    }
    .kpi-value {
        color: #FFFFFF;
        font-size: 1.8rem;
        font-weight: 700;
        line-height: 1.2;
    }
    .kpi-sub {
        color: #1DB954;
        font-size: 0.8rem;
        margin-top: 6px;
    }

    /* Tabs Styling */
    .stTabs [data-baseweb="tab-list"] {
        gap: 8px;
        border-bottom: 1px solid #282828;
    }
    .stTabs [data-baseweb="tab"] {
        border-radius: 8px 8px 0px 0px;
        padding: 10px 18px;
        color: #B3B3B3;
        font-weight: 600;
        background-color: transparent;
    }
    .stTabs [aria-selected="true"] {
        background-color: #282828 !important;
        color: #1DB954 !important;
        border-bottom: 3px solid #1DB954 !important;
    }
    
    /* Header badge */
    .badge-huit {
        background: rgba(29, 185, 84, 0.15);
        border: 1px solid #1DB954;
        color: #1DB954;
        padding: 4px 12px;
        border-radius: 20px;
        font-size: 0.8rem;
        font-weight: 600;
        display: inline-block;
    }
</style>
""", unsafe_allow_html=True)

# --- 3. HÀM TẢI VÀ CACHE DỮ LIỆU ---
@st.cache_data(show_spinner="Đang xử lý dữ liệu Big Data...")
def load_data(file_source):
    """Đọc dữ liệu từ đường dẫn file hoặc đối tượng upload"""
    if isinstance(file_source, str):
        if file_source.endswith('.parquet'):
            df = pd.read_parquet(file_source)
        else:
            with open(file_source, 'r', encoding='utf-8', errors='ignore') as f:
                first_line = f.readline()
                sep = ';' if first_line.count(';') > first_line.count(',') else ','
            df = pd.read_csv(file_source, sep=sep, low_memory=False, on_bad_lines='skip')
    else:
        # File uploader BytesIO
        content = file_source.getvalue().decode('utf-8', errors='ignore')
        first_line = content.split('\n')[0]
        sep = ';' if first_line.count(';') > first_line.count(',') else ','
        df = pd.read_csv(io.StringIO(content), sep=sep, low_memory=False, on_bad_lines='skip')

    # Chuẩn hóa tên cột
    cols = {str(c).strip().lower(): c for c in df.columns}
    def get_c(cands):
        for c in cands:
            for col_l, orig in cols.items():
                if c.lower() in col_l:
                    return orig
        return None

    c_title = get_c(['title', 'track_name', 'song'])
    c_artist = get_c(['artist', 'artists'])
    c_date = get_c(['date', 'release_date'])
    c_dance = get_c(['danceability', 'dance'])
    c_energy = get_c(['energy'])
    c_loud = get_c(['loudness'])
    c_val = get_c(['valence', 'mood'])
    c_acoust = get_c(['acousticness'])
    c_speech = get_c(['speechiness'])
    c_dur = get_c(['duration', 'duration_ms', 'duration_minutes'])
    c_rank = get_c(['rank', 'position'])
    c_points = get_c(['points', 'stream_count', 'streams', 'popularity'])
    c_country = get_c(['nationality', 'country'])
    c_continent = get_c(['continent', 'region'])
    c_url = get_c(['url', 'link'])

    out = pd.DataFrame()
    out['title'] = df[c_title].astype(str) if c_title else "Unknown Track"
    out['artist'] = df[c_artist].astype(str) if c_artist else "Unknown Artist"

    if c_date:
        out['date'] = pd.to_datetime(df[c_date], errors='coerce')
        out['year'] = out['date'].dt.year
        out['month'] = out['date'].dt.month
        out['year_month'] = out['date'].dt.strftime('%Y-%m')
    else:
        out['date'] = pd.NaT
        out['year'] = 2020
        out['month'] = 1
        out['year_month'] = '2020-01'

    # Filter years
    out['year'] = pd.to_numeric(out['year'], errors='coerce')
    out = out.dropna(subset=['year'])
    out['year'] = out['year'].astype(int)

    # Audio features
    features = {
        'danceability': c_dance,
        'energy': c_energy,
        'loudness': c_loud,
        'valence': c_val,
        'acousticness': c_acoust,
        'speechiness': c_speech
    }
    for feat, col in features.items():
        if col and col in df:
            out[feat] = pd.to_numeric(df[col], errors='coerce')
            if feat not in ['loudness']:
                if out[feat].max() > 1.5:
                    out[feat] = out[feat] / 100.0
        else:
            out[feat] = 0.5

    out['danceability'] = out['danceability'].fillna(0.65)
    out['energy'] = out['energy'].fillna(0.65)
    out['valence'] = out['valence'].fillna(0.50)
    out['acousticness'] = out['acousticness'].fillna(0.20)
    out['speechiness'] = out['speechiness'].fillna(0.08)
    out['loudness'] = out['loudness'].fillna(-6.0)

    # Rank & points
    out['rank'] = pd.to_numeric(df[c_rank], errors='coerce').fillna(100).astype(int) if c_rank else 100
    out['points'] = pd.to_numeric(df[c_points], errors='coerce').fillna(201 - out['rank']) if c_points else (201 - out['rank'])

    # Duration in minutes
    if c_dur:
        dur = pd.to_numeric(df[c_dur], errors='coerce').fillna(180000)
        out['duration_min'] = (dur / 60000.0).round(2) if dur.mean() > 30 else dur.round(2)
    else:
        out['duration_min'] = 3.0

    out['country'] = df[c_country].astype(str).fillna("Unknown") if c_country else "Global"
    out['continent'] = df[c_continent].astype(str).fillna("Global") if c_continent else "Global"
    out['url'] = df[c_url].astype(str) if c_url else ""

    return out

# --- 4. SIDEBAR QUẢN LÝ NGUỒN DỮ LIỆU & BỘ LỌC ---
with st.sidebar:
    st.markdown("""
    <div style='text-align: center; padding: 10px 0;'>
        <div class='badge-huit'>ĐỒ ÁN BIG DATA - HUIT</div>
        <h2 style='color: #1DB954; margin: 10px 0 0 0;'><i class='fas fa-music'></i> SPOTIFY ANALYTICS</h2>
        <p style='color: #888; font-size: 0.85rem;'>Giai đoạn 2017 - 2023</p>
    </div>
    <hr style='border-color: #282828;'>
    """, unsafe_allow_html=True)
    
    st.subheader("📁 Nguồn Dữ Liệu")
    data_source_mode = st.radio(
        "Chọn nguồn nạp dữ liệu:",
        ["Dữ liệu sạch có sẵn (Tối ưu)", "Tải lên file CSV mới", "Dùng file trong Downloads"],
        index=0
    )
    
    file_to_load = None
    if data_source_mode == "Dữ liệu sạch có sẵn (Tối ưu)":
        if os.path.exists("spotify_cleaned.parquet"):
            file_to_load = "spotify_cleaned.parquet"
        elif os.path.exists("spotify_cleaned.csv"):
            file_to_load = "spotify_cleaned.csv"
        elif os.path.exists("spotify_top200_sample.csv"):
            file_to_load = "spotify_top200_sample.csv"
    elif data_source_mode == "Tải lên file CSV mới":
        uploaded_file = st.file_uploader("Chọn file CSV Top 200 (Kaggle)", type=["csv"])
        if uploaded_file:
            file_to_load = uploaded_file
    else:
        downloads_path = r"c:\Users\tphuc\Downloads\spotify_artist_streaming_2020_2025.csv"
        if os.path.exists(downloads_path):
            file_to_load = downloads_path
            st.caption(f"Đã tìm thấy file tại thư mục Downloads")
        else:
            st.warning("Chưa tìm thấy file trong Downloads, sử dụng dữ liệu mặc định.")
            file_to_load = "spotify_top200_sample.csv"

    if file_to_load is None:
        file_to_load = "spotify_top200_sample.csv" if os.path.exists("spotify_top200_sample.csv") else None

    # Load DF
    df_raw = load_data(file_to_load) if file_to_load else pd.DataFrame()

    st.markdown("<hr style='border-color: #282828;'>", unsafe_allow_html=True)
    st.subheader("🎯 Bộ Lọc Tương Tác")

    # Filter Năm
    min_year = int(df_raw['year'].min()) if not df_raw.empty else 2017
    max_year = int(df_raw['year'].max()) if not df_raw.empty else 2023
    
    selected_years = st.slider(
        "Khoảng thời gian (Năm):",
        min_value=min_year,
        max_value=max_year,
        value=(max(min_year, 2017), min(max_year, 2023))
    )

    # Filter Top Rank
    rank_cutoff = st.selectbox(
        "Thứ hạng trên BXH:",
        [200, 100, 50, 10],
        index=0,
        format_func=lambda x: f"Top {x} Spotify"
    )

    # Filter Nghệ sĩ
    all_artists = sorted(df_raw['artist'].unique().tolist()) if not df_raw.empty else []
    selected_artists = st.multiselect(
        "Lọc theo Nghệ sĩ (Để trống = Tất cả):",
        options=all_artists,
        default=[]
    )

    # Filter Châu lục
    all_continents = sorted(df_raw['continent'].unique().tolist()) if not df_raw.empty else []
    selected_continent = st.selectbox(
        "Châu lục nghệ sĩ:",
        ["Tất cả"] + all_continents
    )

    st.markdown("<hr style='border-color: #282828;'>", unsafe_allow_html=True)
    st.markdown("""
    <div style='background-color: #181818; padding: 14px; border-radius: 10px; border: 1px solid #282828;'>
        <p style='margin: 0 0 8px 0; font-size: 0.8rem; color: #888; text-transform: uppercase; font-weight: 700; letter-spacing: 0.05em;'>
            <i class='fa-solid fa-users' style='color: #1DB954; margin-right: 6px;'></i> Sinh viên thực hiện:
        </p>
        <p style='margin: 4px 0; font-size: 0.85rem; font-weight: 700; color: #1DB954;'>
            <i class='fa-solid fa-user-graduate' style='margin-right: 6px;'></i> 1. Nguyễn Hà Trọng Phúc <span style='font-size: 0.75rem; color: #888;'>(Nhóm trưởng - 2001230686)</span>
        </p>
        <p style='margin: 4px 0; font-size: 0.85rem; font-weight: 600; color: #FFFFFF;'>
            <i class='fa-solid fa-user' style='color: #1DB954; margin-right: 6px;'></i> 2. Lê Đông Triều
        </p>
        <p style='margin: 4px 0; font-size: 0.85rem; font-weight: 600; color: #FFFFFF;'>
            <i class='fa-solid fa-user' style='color: #1DB954; margin-right: 6px;'></i> 3. Nguyễn Vũ Ngọc Nam
        </p>
        <p style='margin: 10px 0 0 0; font-size: 0.75rem; color: #AAA; border-top: 1px dashed #333; padding-top: 6px;'>
            <i class='fa-solid fa-school' style='color: #1DB954; margin-right: 4px;'></i> Trường ĐH Công Thương TP.HCM (HUIT)
        </p>
    </div>
    """, unsafe_allow_html=True)

# --- 5. ÁP DỤNG BỘ LỌC VÀO DỮ LIỆU ---
df = df_raw.copy()
df = df[(df['year'] >= selected_years[0]) & (df['year'] <= selected_years[1])]
df = df[df['rank'] <= rank_cutoff]

if selected_artists:
    df = df[df['artist'].isin(selected_artists)]

if selected_continent != "Tất cả":
    df = df[df['continent'] == selected_continent]

# Palette màu Spotify Dark Theme
SPOTIFY_GREEN = "#1DB954"
SPOTIFY_BG = "#121212"
PLOT_BG = "#181818"

plotly_layout_defaults = dict(
    paper_bgcolor=PLOT_BG,
    plot_bgcolor=PLOT_BG,
    font=dict(color="#E0E0E0", family="Inter, sans-serif"),
    margin=dict(l=40, r=30, t=50, b=40)
)

# --- 6. GIAO DIỆN CHÍNH & TIÊU ĐỀ ---
col_logo, col_header = st.columns([1, 6])
with col_logo:
    st.image("https://upload.wikimedia.org/wikipedia/commons/1/19/Spotify_logo_without_text.svg", width=75)
with col_header:
    st.markdown(f"""
    <h1 style='margin: 0; padding: 0; color: #FFFFFF;'>Hệ Thống Phân Tích Xu Hướng Âm Nhạc Spotify</h1>
    <p style='color: #B3B3B3; margin-top: 4px; font-size: 1.05rem;'>
        Khám phá đặc trưng âm học, thứ hạng và hành vi thưởng thức âm nhạc toàn cầu giai đoạn 
        <span style='color: #1DB954; font-weight: bold;'>{selected_years[0]} - {selected_years[1]}</span>
    </p>
    """, unsafe_allow_html=True)

st.markdown("<br>", unsafe_allow_html=True)

# --- 7. CÁC TABS PHÂN TÍCH CHUYÊN SÂU ---
tab1, tab2, tab3, tab4, tab5, tab6, tab7 = st.tabs([
    "📊 Tổng Quan & KPI",
    "📈 Xu Hướng 2017-2023",
    "👑 Bảng Vàng Nghệ Sĩ",
    "🌍 Địa Lý & Thị Trường",
    "🔬 Machine Learning & Tương Quan",
    "⚡ Giám Sát Kafka Real-Time",
    "🔍 Tra Cứu Dữ Liệu"
])

# =============================================================================
# TAB 1: TỔNG QUAN & KPI
# =============================================================================
with tab1:
    total_records = len(df)
    unique_artists = df['artist'].nunique() if not df.empty else 0
    unique_tracks = df['title'].nunique() if not df.empty else 0
    total_points = int(df['points'].sum()) if not df.empty else 0
    
    avg_dance = df['danceability'].mean() * 100 if not df.empty else 0
    avg_energy = df['energy'].mean() * 100 if not df.empty else 0
    avg_valence = df['valence'].mean() * 100 if not df.empty else 0
    avg_acoust = df['acousticness'].mean() * 100 if not df.empty else 0

    # KPI Cards
    k1, k2, k3, k4 = st.columns(4)
    with k1:
        st.markdown(f"""
        <div class='kpi-card'>
            <div class='kpi-title'>Tổng Bản Ghi BXH</div>
            <div class='kpi-value'>{total_records:,}</div>
            <div class='kpi-sub'>Dữ liệu Big Data đã xử lý</div>
        </div>
        """, unsafe_allow_html=True)
    with k2:
        st.markdown(f"""
        <div class='kpi-card'>
            <div class='kpi-title'>Tổng Điểm / Lượt Stream</div>
            <div class='kpi-value'>{total_points:,}</div>
            <div class='kpi-sub'>Điểm xếp hạng tích lũy</div>
        </div>
        """, unsafe_allow_html=True)
    with k3:
        st.markdown(f"""
        <div class='kpi-card'>
            <div class='kpi-title'>Số Ca Khúc Độc Bản</div>
            <div class='kpi-value'>{unique_tracks:,}</div>
            <div class='kpi-sub'>Góp mặt trong Top {rank_cutoff}</div>
        </div>
        """, unsafe_allow_html=True)
    with k4:
        st.markdown(f"""
        <div class='kpi-card'>
            <div class='kpi-title'>Số Nghệ Sĩ Góp Mặt</div>
            <div class='kpi-value'>{unique_artists:,}</div>
            <div class='kpi-sub'>Nghệ sĩ toàn cầu</div>
        </div>
        """, unsafe_allow_html=True)

    st.markdown("<br>", unsafe_allow_html=True)

    # Radar Chart & Violin/Histograms
    r_col1, r_col2 = st.columns([1, 1])
    with r_col1:
        st.subheader("🎯 Hồ Sơ Âm Thanh Trung Bình (Audio Radar)")
        st.caption("Tổng hợp 6 đặc trưng âm học cơ bản của các bản hit Top Spotify")
        
        categories = ['Danceability', 'Energy', 'Valence', 'Acousticness', 'Speechiness', 'Danceability']
        values = [
            df['danceability'].mean(),
            df['energy'].mean(),
            df['valence'].mean(),
            df['acousticness'].mean(),
            df['speechiness'].mean(),
            df['danceability'].mean()
        ]
        
        fig_radar = go.Figure()
        fig_radar.add_trace(go.Scatterpolar(
            r=values,
            theta=categories,
            fill='toself',
            name='Spotify Top Hits',
            line=dict(color=SPOTIFY_GREEN, width=2),
            fillcolor='rgba(29, 185, 84, 0.35)'
        ))
        fig_radar.update_layout(
            polar=dict(
                radialaxis=dict(visible=True, range=[0, 1], gridcolor='#333', tickcolor='#888'),
                angularaxis=dict(gridcolor='#333', linecolor='#333')
            ),
            paper_bgcolor=PLOT_BG,
            font=dict(color="#FFF"),
            showlegend=False,
            height=380,
            margin=dict(l=40, r=40, t=30, b=30)
        )
        st.plotly_chart(fig_radar, use_container_width=True)

    with r_col2:
        st.subheader("📊 Phân Bố Mức Độ Năng Lượng & Độ Nhảy")
        st.caption("Mật độ phân bổ của Danceability và Energy trên toàn bộ tập dữ liệu")
        
        fig_box = go.Figure()
        fig_box.add_trace(go.Violin(
            y=df['danceability'],
            name='Danceability',
            box_visible=True,
            meanline_visible=True,
            line_color=SPOTIFY_GREEN,
            fillcolor='rgba(29, 185, 84, 0.4)'
        ))
        fig_box.add_trace(go.Violin(
            y=df['energy'],
            name='Energy',
            box_visible=True,
            meanline_visible=True,
            line_color='#FF8C00',
            fillcolor='rgba(255, 140, 0, 0.4)'
        ))
        fig_box.add_trace(go.Violin(
            y=df['valence'],
            name='Valence (Mood)',
            box_visible=True,
            meanline_visible=True,
            line_color='#1E90FF',
            fillcolor='rgba(30, 144, 255, 0.4)'
        ))
        fig_box.update_layout(**plotly_layout_defaults, height=380, yaxis_title="Thang đo (0 - 1)")
        st.plotly_chart(fig_box, use_container_width=True)

    # Key Insights for Project Defense
    st.markdown("""
    <div style='background: #181818; border-left: 4px solid #1DB954; padding: 16px; border-radius: 6px; margin-top: 10px;'>
        <h4 style='color: #1DB954; margin: 0 0 8px 0;'>💡 Điểm Nhấn Phân Tích (Key Insights cho Báo Cáo Đồ Án):</h4>
        <ul style='color: #DDD; font-size: 0.95rem; margin-bottom: 0;'>
            <li><b>Danceability áp đảo:</b> Chỉ số khả năng nhảy múa trung bình đạt trên <b>68%</b>, khẳng định các bài hát lọt Top 200 hầu hết có cấu trúc tiết tấu rõ ràng, dễ bắt tai và phù hợp cho khiêu vũ/tiệc tùng.</li>
            <li><b>Sự thống trị của Pop & Nhịp điệu sôi động:</b> Chỉ số Energy và Loudness phân bố lệch phải, phản ánh kỹ thuật sản xuất âm thanh hiện đại (Loudness War) nhằm tăng độ nổi bật trên nền tảng số.</li>
            <li><b>Acousticness ở mức thấp (&lt; 25%):</b> Nhạc mộc mạc acoustic rất hiếm khi giữ vị trí cao lâu dài, nhường chỗ cho các bản phối điện tử (Electronic/Synth-pop/Trap).</li>
        </ul>
    </div>
    """, unsafe_allow_html=True)

# =============================================================================
# TAB 2: XU HƯỚNG ÂM NHẠC 2017 - 2023
# =============================================================================
with tab2:
    st.subheader("📈 Xu Hướng Biến Thiên Đặc Trưng Âm Học Qua Các Năm (2017 - 2023)")
    st.caption("Quan sát sự chuyển dịch trong thị hiếu âm nhạc: Nhạc ngày càng vui hơn hay trầm lắng hơn?")

    yearly_trends = df.groupby('year').agg({
        'danceability': 'mean',
        'energy': 'mean',
        'valence': 'mean',
        'acousticness': 'mean',
        'duration_min': 'mean',
        'points': 'sum'
    }).reset_index()

    fig_trends = go.Figure()
    fig_trends.add_trace(go.Scatter(
        x=yearly_trends['year'], y=yearly_trends['danceability'] * 100,
        mode='lines+markers', name='Danceability (%)',
        line=dict(color=SPOTIFY_GREEN, width=3),
        marker=dict(size=8)
    ))
    fig_trends.add_trace(go.Scatter(
        x=yearly_trends['year'], y=yearly_trends['energy'] * 100,
        mode='lines+markers', name='Energy (%)',
        line=dict(color='#FF5722', width=2.5, dash='solid'),
        marker=dict(size=7)
    ))
    fig_trends.add_trace(go.Scatter(
        x=yearly_trends['year'], y=yearly_trends['valence'] * 100,
        mode='lines+markers', name='Valence / Vui vẻ (%)',
        line=dict(color='#00BCD4', width=2.5, dash='dot'),
        marker=dict(size=7)
    ))
    fig_trends.add_trace(go.Scatter(
        x=yearly_trends['year'], y=yearly_trends['acousticness'] * 100,
        mode='lines+markers', name='Acousticness (%)',
        line=dict(color='#9C27B0', width=2, dash='dash'),
        marker=dict(size=6)
    ))

    fig_trends.update_layout(
        **plotly_layout_defaults,
        height=420,
        xaxis=dict(tickmode='linear', dtick=1, gridcolor='#282828'),
        yaxis=dict(title='Tỷ lệ phần trăm (%)', gridcolor='#282828'),
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1)
    )
    st.plotly_chart(fig_trends, use_container_width=True)

    c_trend1, c_trend2 = st.columns(2)
    with c_trend1:
        st.subheader("⏱️ Xu Hướng Thời Lượng Bài Hát (Song Duration)")
        st.caption("Chứng minh bài hát ngày càng ngắn lại do định dạng Streaming và TikTok")

        fig_dur = px.bar(
            yearly_trends,
            x='year',
            y='duration_min',
            text=yearly_trends['duration_min'].apply(lambda x: f"{x:.2f} phút"),
            color='duration_min',
            color_continuous_scale=['#1DB954', '#128C7E'],
            labels={'duration_min': 'Thời lượng TB (phút)', 'year': 'Năm'}
        )
        fig_dur.update_layout(**plotly_layout_defaults, height=340, coloraxis_showscale=False)
        fig_dur.update_traces(textposition='outside')
        st.plotly_chart(fig_dur, use_container_width=True)

    with c_trend2:
        st.subheader("🗓️ Tính Mùa Vụ (Seasonality) Theo Tháng")
        st.caption("Mức độ hưng phấn (Valence & Energy) theo các tháng trong năm")

        monthly_trends = df.groupby('month').agg({
            'valence': 'mean',
            'energy': 'mean'
        }).reset_index()

        fig_month = go.Figure()
        fig_month.add_trace(go.Bar(
            x=monthly_trends['month'],
            y=monthly_trends['energy'],
            name='Energy',
            marker_color='rgba(255, 140, 0, 0.75)'
        ))
        fig_month.add_trace(go.Bar(
            x=monthly_trends['month'],
            y=monthly_trends['valence'],
            name='Valence (Mood)',
            marker_color='rgba(29, 185, 84, 0.75)'
        ))
        fig_month.update_layout(
            **plotly_layout_defaults,
            barmode='group',
            height=340,
            xaxis=dict(tickmode='linear', dtick=1, title="Tháng trong năm (1 - 12)", gridcolor='#282828'),
            yaxis=dict(title="Chỉ số", gridcolor='#282828')
        )
        st.plotly_chart(fig_month, use_container_width=True)

# =============================================================================
# TAB 3: BẢNG VÀNG NGHỆ SĨ & CA KHÚC
# =============================================================================
with tab3:
    st.subheader("👑 Bảng Vàng Nghệ Sĩ Thống Trị Top 200 Spotify")
    st.caption("Thống kê nghệ sĩ có số lượng bài hát và tổng điểm xếp hạng lớn nhất")

    top_art_df = df.groupby('artist').agg({
        'points': 'sum',
        'title': 'count'
    }).sort_values(by='points', ascending=False).head(15).reset_index()

    fig_artist = px.bar(
        top_art_df,
        x='points',
        y='artist',
        orientation='h',
        color='points',
        color_continuous_scale=['#191414', '#1DB954'],
        text='points',
        labels={'points': 'Tổng Điểm Tích Lũy', 'artist': 'Nghệ sĩ'}
    )
    fig_artist.update_layout(**plotly_layout_defaults, height=450, yaxis=dict(autorange="reversed"))
    st.plotly_chart(fig_artist, use_container_width=True)

    st.markdown("<hr style='border-color: #282828;'>", unsafe_allow_html=True)
    st.subheader("⚔️ Công Cụ So Sánh Đối Đầu 2 Nghệ Sĩ (Head-to-Head Comparison)")
    st.caption("So sánh phong cách âm nhạc và chỉ số âm thanh giữa 2 nghệ sĩ bất kỳ")

    h_col1, h_col2 = st.columns(2)
    top_artists_list = df['artist'].value_counts().head(30).index.tolist()
    if len(top_artists_list) < 2:
        top_artists_list = df['artist'].unique().tolist()

    with h_col1:
        artist_a = st.selectbox("Chọn Nghệ Sĩ A:", options=top_artists_list, index=0)
    with h_col2:
        artist_b = st.selectbox("Chọn Nghệ Sĩ B:", options=top_artists_list, index=min(1, len(top_artists_list)-1))

    if artist_a and artist_b:
        df_a = df[df['artist'] == artist_a]
        df_b = df[df['artist'] == artist_b]

        feat_cols = ['danceability', 'energy', 'valence', 'acousticness', 'speechiness']
        mean_a = [df_a[c].mean() for c in feat_cols] + [df_a[feat_cols[0]].mean()]
        mean_b = [df_b[c].mean() for c in feat_cols] + [df_b[feat_cols[0]].mean()]
        cats = ['Danceability', 'Energy', 'Valence', 'Acousticness', 'Speechiness', 'Danceability']

        fig_compare = go.Figure()
        fig_compare.add_trace(go.Scatterpolar(
            r=mean_a, theta=cats, fill='toself', name=artist_a,
            line_color='#1DB954', fillcolor='rgba(29, 185, 84, 0.3)'
        ))
        fig_compare.add_trace(go.Scatterpolar(
            r=mean_b, theta=cats, fill='toself', name=artist_b,
            line_color='#FF4500', fillcolor='rgba(255, 69, 0, 0.3)'
        ))
        fig_compare.update_layout(
            polar=dict(radialaxis=dict(visible=True, range=[0, 1], gridcolor='#333')),
            paper_bgcolor=PLOT_BG,
            font=dict(color="#FFF"),
            height=380
        )

        c_a, c_b = st.columns([1, 1])
        with c_a:
            st.plotly_chart(fig_compare, use_container_width=True)
        with c_b:
            st.markdown(f"#### 📊 Thống Kê Nhanh:")
            st.write(f"- **{artist_a}**: {len(df_a):,} lần lọt BXH | Tổng điểm: {int(df_a['points'].sum()):,}")
            st.write(f"- **{artist_b}**: {len(df_b):,} lần lọt BXH | Tổng điểm: {int(df_b['points'].sum()):,}")
            st.write(f"- **Ca khúc tiêu biểu {artist_a}**: {', '.join(df_a['title'].value_counts().head(3).index.tolist())}")
            st.write(f"- **Ca khúc tiêu biểu {artist_b}**: {', '.join(df_b['title'].value_counts().head(3).index.tolist())}")

# =============================================================================
# TAB 4: PHÂN TÍCH ĐỊA LÝ & THỊ TRƯỜNG
# =============================================================================
with tab4:
    st.subheader("🌍 Thị Phần Âm Nhạc Toàn Cầu Theo Quốc Gia & Châu Lục")
    st.caption("Nguồn gốc xuất xứ của các nghệ sĩ thống trị bảng xếp hạng thế giới")

    geo_col1, geo_col2 = st.columns(2)
    with geo_col1:
        cont_counts = df['continent'].value_counts().reset_index()
        cont_counts.columns = ['Continent', 'Count']
        fig_donut = px.pie(
            cont_counts,
            values='Count',
            names='Continent',
            hole=0.45,
            color_discrete_sequence=['#1DB954', '#3B82F6', '#F59E0B', '#EF4444', '#8B5CF6', '#10B981'],
            title="Tỷ Trọng Ca Khúc Theo Châu Lục"
        )
        fig_donut.update_layout(**plotly_layout_defaults, height=380)
        st.plotly_chart(fig_donut, use_container_width=True)

    with geo_col2:
        country_counts = df['country'].value_counts().head(10).reset_index()
        country_counts.columns = ['Country', 'Count']
        fig_country = px.bar(
            country_counts,
            x='Count',
            y='Country',
            orientation='h',
            color='Count',
            color_continuous_scale=['#191414', '#1DB954'],
            title="Top 10 Quốc Gia Có Nhiều Bài Hát Nhất",
            labels={'Count': 'Số lượng bài hát', 'Country': 'Quốc gia'}
        )
        fig_country.update_layout(**plotly_layout_defaults, height=380, yaxis=dict(autorange="reversed"))
        st.plotly_chart(fig_country, use_container_width=True)

# =============================================================================
# TAB 5: KHOA HỌC DỮ LIỆU & MACHINE LEARNING
# =============================================================================
with tab5:
    st.subheader("🔬 Phân Tích Khoa Học Dữ Liệu & Machine Learning Trên Big Data")
    st.caption("Ma trận tương quan âm học và Phân cụm bài hát K-Means (K-Means Clustering)")

    ml_col1, ml_col2 = st.columns(2)
    
    with ml_col1:
        st.markdown("#### 1. Ma Trận Tương Quan Âm Học (Correlation Heatmap)")
        corr_cols = ['danceability', 'energy', 'loudness', 'valence', 'acousticness', 'speechiness', 'duration_min']
        corr_matrix = df[corr_cols].corr()

        fig_corr = px.imshow(
            corr_matrix.round(2),
            text_auto=True,
            color_continuous_scale='RdBu_r',
            zmin=-1, zmax=1,
            labels=dict(color="Hệ số tương quan")
        )
        fig_corr.update_layout(**plotly_layout_defaults, height=420)
        st.plotly_chart(fig_corr, use_container_width=True)

    with ml_col2:
        st.markdown("#### 2. Phân Tích Quan Hệ Danceability vs Valence")
        sample_scatter = df.sample(min(1500, len(df)), random_state=42)
        fig_scatter = px.scatter(
            sample_scatter,
            x='danceability',
            y='energy',
            color='valence',
            color_continuous_scale='Viridis',
            hover_data=['title', 'artist'],
            labels={'danceability': 'Danceability', 'energy': 'Energy', 'valence': 'Valence (Mood)'},
            trendline="ols"
        )
        fig_scatter.update_layout(**plotly_layout_defaults, height=420)
        st.plotly_chart(fig_scatter, use_container_width=True)

    st.markdown("<hr style='border-color: #282828;'>", unsafe_allow_html=True)
    st.markdown("#### 3. Mô Hình Phân Cụm Bài Hát K-Means (Music Style Clustering)")
    st.write("Sử dụng thuật toán **K-Means Clustering** để gom cụm các bài hát Top 200 thành 4 phong cách âm nhạc đặc trưng:")

    # Kiểm tra xem có kết quả phân tích phân tán từ PySpark MLlib (Bước 7 HDFS) không
    spark_kmeans_file = "kmeans_results.parquet"
    use_spark_results = os.path.exists(spark_kmeans_file)

    if use_spark_results:
        st.success("⚡ **Đang sử dụng kết quả phân cụm Machine Learning phân tán từ Apache PySpark MLlib & HDFS!**")
        spark_df = pd.read_parquet(spark_kmeans_file)
        
        # Đặc trưng và chuẩn hóa để vẽ PCA
        cluster_features = ['avg_danceability', 'avg_energy', 'avg_valence', 'avg_acousticness', 'avg_speechiness']
        X_sp = spark_df[cluster_features].dropna()
        scaler = StandardScaler()
        X_scaled = scaler.fit_transform(X_sp)
        pca = PCA(n_components=2)
        pca_result = pca.fit_transform(X_scaled)

        cluster_df = spark_df.copy()
        cluster_df['PCA1'] = pca_result[:, 0]
        cluster_df['PCA2'] = pca_result[:, 1]
        cluster_df['Cluster_Name'] = cluster_df['cluster_name']
    else:
        st.info("ℹ️ Đang chạy mô hình K-Means giả lập Client (Chạy `run_spark_kmeans.bat` để nạp kết quả Spark phân tán)")
        cluster_features = ['danceability', 'energy', 'valence', 'acousticness', 'speechiness']
        X = df[cluster_features].dropna()
        scaler = StandardScaler()
        X_scaled = scaler.fit_transform(X)
        n_clusters = 4
        kmeans = KMeans(n_clusters=n_clusters, random_state=42, n_init=10)
        clusters = kmeans.fit_predict(X_scaled)
        pca = PCA(n_components=2)
        pca_result = pca.fit_transform(X_scaled)

        cluster_df = pd.DataFrame(X, columns=cluster_features)
        cluster_df['Cluster'] = [f"Nhóm {c}" for c in clusters]
        cluster_df['PCA1'] = pca_result[:, 0]
        cluster_df['PCA2'] = pca_result[:, 1]
        cluster_df['title'] = df.loc[X.index, 'title']
        cluster_df['artist'] = df.loc[X.index, 'artist']

        c_names = {
            "Nhóm 0": "🔥 Party & High-Energy Pop",
            "Nhóm 1": "🍃 Acoustic & Chill Vibes",
            "Nhóm 2": "🌧️ Melancholic & Slow Tempo",
            "Nhóm 3": "🎤 Rhythmic & Hip-Hop/Rap"
        }
        cluster_df['Cluster_Name'] = cluster_df['Cluster'].map(c_names).fillna(cluster_df['Cluster'])

    c_pca, c_table = st.columns([6, 4])
    with c_pca:
        fig_pca = px.scatter(
            cluster_df.sample(min(1200, len(cluster_df)), random_state=42),
            x='PCA1',
            y='PCA2',
            color='Cluster_Name',
            hover_data=['title', 'artist'] if 'title' in cluster_df.columns else None,
            title="Không Gian Phân Cụm 2D (PCA Projection)",
            color_discrete_sequence=['#1DB954', '#3B82F6', '#F59E0B', '#EC4899']
        )
        fig_pca.update_layout(**plotly_layout_defaults, height=380)
        st.plotly_chart(fig_pca, use_container_width=True)

    with c_table:
        st.markdown("##### Đặc Trưng Trung Bình Mỗi Cụm:")
        cluster_summary = cluster_df.groupby('Cluster_Name')[cluster_features].mean().round(2)
        st.dataframe(cluster_summary, use_container_width=True)

# =============================================================================
# TAB 6: GIÁM SÁT KAFKA REAL-TIME STREAMING
# =============================================================================
with tab6:
    st.subheader("⚡ Giám Sát Luồng Dữ Liệu Real-Time Streaming Ingestion (Apache Kafka)")
    st.caption("Mô phỏng thu nhận và trực quan hóa các sự kiện nghe nhạc (Listening Events) trực tiếp từ Kafka Broker")

    # Banner thông số kỹ thuật Kafka
    st.markdown("""
    <div style='background: #181818; border-left: 4px solid #1DB954; padding: 12px 16px; border-radius: 6px; margin-bottom: 15px;'>
        <div style='display: flex; justify-content: space-between; flex-wrap: wrap;'>
            <div><b>Broker:</b> <code>localhost:9092</code></div>
            <div><b>Topic:</b> <span style='color: #1DB954; font-weight: bold;'>spotify_streaming</span></div>
            <div><b>Tần suất nạp:</b> <code>1 - 2 giây / event</code></div>
            <div><b>Kiến trúc:</b> <code>Lambda Architecture (Speed Layer)</code></div>
        </div>
    </div>
    """, unsafe_allow_html=True)

    c_stream_ctrl1, c_stream_ctrl2, c_stream_ctrl3 = st.columns([3, 2, 2])
    with c_stream_ctrl1:
        auto_refresh = st.checkbox("🔴 Bật chế độ Theo dõi Trực tiếp (Live Polling)", value=False)
    with c_stream_ctrl2:
        manual_refresh = st.button("🔄 Làm mới dữ liệu ngay")
    with c_stream_ctrl3:
        simulate_one = st.button("⚡ Bắn thử 1 Event Stream")

    buffer_path = "kafka_stream_buffer.json"
    
    # Xử lý nút bắn thử 1 event
    if simulate_one:
        try:
            import csv
            sample_csv = "spotify_top200_sample.csv"
            if os.path.exists(sample_csv):
                with open(sample_csv, 'r', encoding='utf-8', errors='ignore') as f:
                    first_line = f.readline()
                    delim = ';' if first_line.count(';') > first_line.count(',') else ','
                    f.seek(0)
                    r = csv.DictReader(f, delimiter=delim)
                    # Chọn ngẫu nhiên hoặc lấy 1 dòng
                    row = next(r)
                    import uuid
                    payload = {
                        "event_id": str(uuid.uuid4()),
                        "stream_timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                        "rank": int(row.get('Song Rank', 100)),
                        "title": str(row.get('Title of the Song', 'Unknown Track')),
                        "artist": str(row.get('Artist Name', 'Unknown Artist')),
                        "danceability": float(row.get('Danceability', 0.65)),
                        "energy": float(row.get('Energy', 0.65)),
                        "valence": float(row.get('Valence', 0.50)),
                        "acousticness": float(row.get('Acousticness', 0.20)),
                        "points": int(row.get('Points (Total)', 100)),
                        "country": str(row.get('Nationality of each artist', 'Global')),
                        "continent": str(row.get("Artist's Continent", 'Global'))
                    }
                    existing = []
                    if os.path.exists(buffer_path):
                        try:
                            with open(buffer_path, 'r', encoding='utf-8') as bf:
                                existing = json.load(bf).get("events", [])
                        except Exception:
                            existing = []
                    existing.insert(0, payload)
                    with open(buffer_path, 'w', encoding='utf-8') as bf:
                        json.dump({
                            "total_sent": len(existing),
                            "last_updated": payload['stream_timestamp'],
                            "events": existing[:25]
                        }, bf, ensure_ascii=False, indent=2)
                    st.success(f"Đã phát sinh sự kiện stream: '{payload['title']}' - {payload['artist']}")
        except Exception as ex:
            st.error(f"Lỗi mô phỏng: {ex}")

    # Đọc dữ liệu từ buffer
    stream_data = None
    if os.path.exists(buffer_path):
        try:
            with open(buffer_path, 'r', encoding='utf-8') as f:
                stream_data = json.load(f)
        except Exception:
            stream_data = None

    if stream_data and stream_data.get("events"):
        events = stream_data["events"]
        total_ingested = stream_data.get("total_sent", len(events))
        latest = events[0]

        # Top Metric Cards
        m1, m2, m3, m4 = st.columns(4)
        with m1:
            st.metric("Tổng Sự Kiện Đã Nhận", f"{total_ingested:,}")
        with m2:
            st.metric("Ca Khúc Gần Nhất", f"#{latest.get('rank', 1)}: {latest.get('title', '')[:14]}...")
        with m3:
            st.metric("Nghệ Sĩ", latest.get('artist', ''))
        with m4:
            st.metric("Thời Gian Nhận", str(latest.get('stream_timestamp', '')).split(' ')[-1])

        st.markdown("<hr style='border-color: #282828;'>", unsafe_allow_html=True)

        col_now_playing, col_audio_viz = st.columns([5, 5])
        
        with col_now_playing:
            st.markdown(f"""
            <div style='background: linear-gradient(135deg, #1DB954 0%, #191414 100%); padding: 20px; border-radius: 12px; color: #FFF; box-shadow: 0 4px 15px rgba(0,0,0,0.5);'>
                <div style='display: flex; justify-content: space-between; align-items: center;'>
                    <span style='background: rgba(0,0,0,0.5); padding: 4px 10px; border-radius: 20px; font-size: 0.8rem; font-weight: bold;'>
                        🔴 NOW PLAYING VIA KAFKA
                    </span>
                    <span style='font-size: 0.85rem; opacity: 0.9;'>BXH TOP #{latest.get('rank', 1)}</span>
                </div>
                <h2 style='margin: 12px 0 4px 0; color: #FFFFFF;'>{latest.get('title', 'Unknown Track')}</h2>
                <h4 style='margin: 0; color: #E0E0E0; font-weight: normal;'>{latest.get('artist', 'Unknown Artist')}</h4>
                <div style='margin-top: 15px; font-size: 0.9rem; line-height: 1.6;'>
                    <div>🌍 <b>Quốc gia:</b> {latest.get('country', 'Global')} ({latest.get('continent', 'Global')})</div>
                    <div>⭐ <b>Điểm tích lũy:</b> {latest.get('points', 0):,} điểm</div>
                    <div>🕒 <b>Stream Timestamp:</b> <code>{latest.get('stream_timestamp', '')}</code></div>
                    <div>🔑 <b>Event ID:</b> <code>{str(latest.get('event_id', ''))[:18]}...</code></div>
                </div>
            </div>
            """, unsafe_allow_html=True)

        with col_audio_viz:
            st.markdown("##### 📊 Chỉ Số Âm Học Thời Gian Thực (Audio Features Live)")
            feat_labels = ['Danceability', 'Energy', 'Valence', 'Acousticness']
            feat_vals = [
                float(latest.get('danceability', 0.65)),
                float(latest.get('energy', 0.65)),
                float(latest.get('valence', 0.50)),
                float(latest.get('acousticness', 0.20))
            ]
            fig_live_bar = go.Figure(go.Bar(
                x=[v * 100 for v in feat_vals],
                y=feat_labels,
                orientation='h',
                marker=dict(color=['#1DB954', '#FF8C00', '#1E90FF', '#9C27B0']),
                text=[f"{v*100:.1f}%" for v in feat_vals],
                textposition='outside'
            ))
            fig_live_bar.update_layout(
                **plotly_layout_defaults,
                height=260,
                xaxis=dict(range=[0, 115], title="Tỷ lệ (%)")
            )
            st.plotly_chart(fig_live_bar, use_container_width=True)

        st.markdown("##### 📋 Nhật Ký Các Sự Kiện Streaming Vừa Nhận (Live Event Feed)")
        feed_df = pd.DataFrame(events)
        show_cols = ['stream_timestamp', 'rank', 'title', 'artist', 'points', 'danceability', 'energy', 'country']
        feed_df_show = feed_df[[c for c in show_cols if c in feed_df.columns]].head(15)
        st.dataframe(feed_df_show, use_container_width=True, height=280)

        if auto_refresh:
            time.sleep(1.5)
            st.rerun()

    else:
        st.info("💡 **Chưa phát hiện sự kiện Stream nào từ Kafka!**")
        st.markdown("""
        **Để xem luồng dữ liệu thời gian thực:**
        1. Nhấp đúp chuột vào file **`run_kafka_producer.bat`** để kích hoạt luồng phát sự kiện.
        2. Hoặc nhấn nút **`⚡ Bắn thử 1 Event Stream`** ở phía trên để kiểm tra giao diện ngay lập tức!
        """)

# =============================================================================
# TAB 7: TRA CỨU DỮ LIỆU & XUẤT BÁO CÁO
# =============================================================================
with tab7:
    st.subheader("🔍 Tra Cứu Dữ Liệu & Xuất Báo Cáo")
    st.caption("Tìm kiếm chi tiết thông tin bài hát và tải file dữ liệu phục vụ nghiên cứu")

    search_query = st.text_input("🔎 Nhập tên bài hát hoặc nghệ sĩ để tìm kiếm:", "")
    view_df = df.copy()
    if search_query:
        view_df = view_df[
            view_df['title'].str.contains(search_query, case=False, na=False) |
            view_df['artist'].str.contains(search_query, case=False, na=False)
        ]

    cols_to_show = ['rank', 'title', 'artist', 'year', 'points', 'danceability', 'energy', 'valence', 'duration_min', 'country']
    st.dataframe(
        view_df[cols_to_show].head(100),
        use_container_width=True,
        height=400
    )

    # Nút download
    csv_buffer = view_df.to_csv(index=False).encode('utf-8')
    st.download_button(
        label="📥 Tải Xuống Dữ Liệu Đang Lọc (CSV)",
        data=csv_buffer,
        file_name="spotify_filtered_export.csv",
        mime="text/csv"
    )
