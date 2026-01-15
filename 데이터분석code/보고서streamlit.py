
import streamlit as st
import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt
import glob
import os
import platform
from matplotlib import font_manager, rc

# 1. Page Config
st.set_page_config(page_title="흑백요리사 쉐프 트렌드 분석", layout="wide")

# 2. Font Settings for Korean
def set_korean_font():
    system_name = platform.system()
    if system_name == "Windows":
        # Check standard Windows font paths
        font_path = "c:/Windows/Fonts/malgun.ttf"
        try:
            font_name = font_manager.FontProperties(fname=font_path).get_name()
            rc('font', family=font_name)
        except:
            # Fallback if specific file not found
            plt.rcParams['font.family'] = 'Malgun Gothic'
    elif system_name == "Darwin":  # Mac
        rc('font', family="AppleGothic")
    else:  # Linux
        rc('font', family="NanumGothic")
        
    plt.rcParams['axes.unicode_minus'] = False

set_korean_font()

# 3. Data Loading
@st.cache_data
def load_data():
    base_path = r"c:\Users\USER\Documents\웅진씽크빅kdt\흑백요리사\흑백요리사트렌드추이"
    
    if not os.path.exists(base_path):
        st.error(f"데이터 경로를 찾을 수 없습니다: {base_path}")
        return pd.DataFrame()

    datalab_files = glob.glob(os.path.join(base_path, "*_datalab.csv"))
    all_data = []

    for f_naver in datalab_files:
        try:
            # Extract Chef ID
            filename = os.path.basename(f_naver)
            chef_id = filename.replace("_datalab.csv", "")
            
            # Determine corresponding Google file
            f_google = os.path.join(base_path, f"{chef_id}_google.csv")
            
            if not os.path.exists(f_google):
                continue
                
            # Read Naver Data
            try:
                df_naver = pd.read_csv(f_naver, encoding='utf-8')
            except UnicodeDecodeError:
                df_naver = pd.read_csv(f_naver, encoding='cp949')
                
            if df_naver.shape[1] < 2:
                continue
                
            chef_name = df_naver.columns[1]
            
            # Standardize Naver
            df_naver = df_naver.rename(columns={df_naver.columns[0]: 'Date', df_naver.columns[1]: 'Value'})
            df_naver['Source'] = 'Naver'
            df_naver['Chef'] = chef_name
            
            # Read Google Data
            try:
                df_google = pd.read_csv(f_google, encoding='utf-8')
            except UnicodeDecodeError:
                df_google = pd.read_csv(f_google, encoding='cp949')
                
            if df_google.shape[1] < 2:
                continue
                
            # Standardize Google
            df_google = df_google.rename(columns={df_google.columns[0]: 'Date', df_google.columns[1]: 'Value'})
            df_google['Source'] = 'Google'
            df_google['Chef'] = chef_name
            
            # Clean Data
            df_naver = df_naver.dropna(subset=['Value'])
            df_google = df_google.dropna(subset=['Value'])
            
            df_naver['Value'] = pd.to_numeric(df_naver['Value'], errors='coerce')
            df_google['Value'] = pd.to_numeric(df_google['Value'], errors='coerce')
            
            all_data.extend([df_naver, df_google])
            
        except Exception as e:
            print(f"Error processing {f_naver}: {e}")

    if not all_data:
        return pd.DataFrame()

    final_df = pd.concat(all_data, ignore_index=True)
    final_df['Date'] = pd.to_datetime(final_df['Date'])
    
    return final_df

# 4. App UI
st.title("👨‍🍳 흑백요리사 쉐프 검색 트렌드 분석")
st.markdown("네이버와 구글의 검색 트렌드 데이터를 시각화합니다.")

# Load Data
df = load_data()

if df.empty:
    st.warning("데이터를 불러올 수 없습니다.")
else:
    # Sidebar Filters
    st.sidebar.header("설정")
    
    # Chef Selection
    all_chefs = sorted(df['Chef'].unique())
    selected_chefs = st.sidebar.multiselect(
        "쉐프 선택 (비워두면 전체 보기)",
        options=all_chefs,
        default=[]
    )
    
    # Filter Data
    if selected_chefs:
        plot_df = df[df['Chef'].isin(selected_chefs)]
    else:
        plot_df = df
        
    # Main Plot (Seaborn)
    st.subheader("📈 트렌드 차트")
    st.markdown("쉐프별 네이버(초록색)와 구글(파란색) 검색 트렌드 추이를 비교한 그래프입니다.")
    
    if not plot_df.empty:
        # Determine height based on number of chefs if wrapping
        num_chefs = plot_df['Chef'].nunique()
        col_wrap = 4
        
        # Create FacetGrid
        g = sns.relplot(
            data=plot_df,
            x="Date", y="Value",
            hue="Source",
            col="Chef",
            kind="line",
            palette={'Google': 'blue', 'Naver': 'green'},
            col_wrap=col_wrap,
            height=4,
            aspect=1.5,
            facet_kws={'sharey': False, 'sharex': True}
        )
        
        # Adjust Title and Layout
        # g.fig.suptitle('검색 트렌드 (Naver vs Google)', size=16) 
        # Title is handled by streamlit header, but we can keep it inside plot if needed.
        # Removing internal title to fit streamlit better or adjust top margin
        g.fig.subplots_adjust(top=0.9)
        
        # Rotate x-axis labels
        for axes in g.axes.flat:
            _ = axes.tick_params(axis='x', rotation=45)
            
        st.pyplot(g.fig)
    else:
        st.info("선택된 데이터가 없습니다.")

    # Data Display
    with st.expander("📊 원본 데이터 보기"):
        st.dataframe(plot_df)

    # Download Button
    csv = plot_df.to_csv(index=False).encode('utf-8-sig') # utf-8-sig for Excel
    st.download_button(
        label="CSV로 다운로드",
        data=csv,
        file_name='chef_trends.csv',
        mime='text/csv',
    )
