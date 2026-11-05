"""
흑백요리사2 통합 분석 대시보드
- 방송 효과 분석 (방영일 기준 리뷰 및 유동인구 변화)
- 통계 분석 (심사위원 예측, 장르별 생존율, 트렌드)
"""
import streamlit as st
import pandas as pd
import numpy as np
import sys
import os
import platform

# 모듈 경로 추가
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.append(SCRIPT_DIR)

# 데이터 경로 헬퍼 함수 (Streamlit Cloud 호환)
def get_data_path(filename):
    """데이터 파일 경로 찾기"""
    # 1순위: data/ 폴더
    data_path = os.path.join(SCRIPT_DIR, 'data', filename)
    if os.path.exists(data_path):
        return data_path
    # 2순위: 상위 폴더
    parent_path = os.path.join(os.path.dirname(SCRIPT_DIR), filename)
    if os.path.exists(parent_path):
        return parent_path
    # 3순위: 현재 폴더
    current_path = os.path.join(SCRIPT_DIR, filename)
    if os.path.exists(current_path):
        return current_path
    return data_path  # 기본값

from data_processor import (
    load_restaurants,
    BROADCAST_DATES
)

# === 한글 폰트 설정 ===
def set_korean_font():
    """한글 폰트 설정 (Windows/Mac/Linux 호환)"""
    global plt, sns
    import matplotlib.pyplot as plt
    import matplotlib.font_manager as fm
    import seaborn as sns

    system_name = platform.system()

    if system_name == "Windows":
        # Windows - 맑은 고딕
        font_path = "c:/Windows/Fonts/malgun.ttf"
        if os.path.exists(font_path):
            font_name = fm.FontProperties(fname=font_path, size=10).get_name()
        else:
            font_name = 'Malgun Gothic'
    elif system_name == "Darwin":
        # Mac - 애플고딕
        font_path = '/System/Library/Fonts/AppleGothic.ttf'
        if os.path.exists(font_path):
            font_name = fm.FontProperties(fname=font_path, size=10).get_name()
        else:
            font_name = 'AppleGothic'
    else:
        # Linux (Streamlit Cloud) - 나눔고딕
        font_path = '/usr/share/fonts/truetype/nanum/NanumGothic.ttf'
        if os.path.exists(font_path):
            # 폰트 매니저에 명시적으로 추가
            fm.fontManager.addfont(font_path)
            font_name = fm.FontProperties(fname=font_path, size=10).get_name()
        else:
            font_name = 'NanumGothic'

    # matplotlib 폰트 설정
    plt.rc('font', family=font_name)
    plt.rcParams['axes.unicode_minus'] = False

    # 밝은 배경 설정 (가시성 개선)
    plt.rcParams['figure.facecolor'] = 'white'
    plt.rcParams['axes.facecolor'] = 'white'
    plt.rcParams['savefig.facecolor'] = 'white'
    plt.rcParams['axes.edgecolor'] = 'black'
    plt.rcParams['axes.labelcolor'] = 'black'
    plt.rcParams['xtick.color'] = 'black'
    plt.rcParams['ytick.color'] = 'black'
    plt.rcParams['text.color'] = 'black'

    # seaborn 폰트 설정 (매우 중요!)
    sns.set_style("whitegrid")
    sns.set_palette("bright")
    sns.set(font=font_name, rc={'axes.unicode_minus': False})

# === 페이지 설정 ===
st.set_page_config(
    page_title="흑백요리사 통합 분석 대시보드",
    page_icon="🍳",
    layout="wide",
    initial_sidebar_state="expanded"
)

# === 커스텀 CSS ===
st.markdown("""
<style>
    .main-header {
        font-size: 2.5rem;
        font-weight: bold;
        color: #1f1f1f;
        text-align: center;
        margin-bottom: 1rem;
    }
    .sub-header {
        font-size: 1.2rem;
        color: #666;
        text-align: center;
        margin-bottom: 2rem;
    }
    .metric-card {
        background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
        padding: 1rem;
        border-radius: 10px;
        color: white;
        text-align: center;
    }
    .stTabs [data-baseweb="tab-list"] {
        gap: 24px;
    }
    .stTabs [data-baseweb="tab"] {
        font-size: 1rem;
        font-weight: 600;
    }
</style>
""", unsafe_allow_html=True)

# === 데이터 캐싱 ===
@st.cache_data(show_spinner=False)
def load_review_changes():
    """Load precomputed restaurant and episode review counts."""
    return pd.read_csv(get_data_path('review_changes.csv'))

@st.cache_data(show_spinner=False)
def load_daily_population():
    """Load precomputed daily district totals instead of the 38 MB raw CSV."""
    df = pd.read_csv(get_data_path('daily_population_by_district.csv'))
    df['date'] = pd.to_datetime(df['date'])
    return df

@st.cache_data(show_spinner=False)
def load_restaurant_details():
    return load_restaurants()

@st.cache_data(show_spinner=False)
def load_survival_results():
    """This frozen portfolio snapshot matches the published survival table."""
    return pd.read_csv(get_data_path('셰프서바이벌결과요약.csv'))

@st.cache_data
def load_survival_data():
    """서바이벌 데이터 로드"""
    df = load_survival_results()
    if df is None or df.empty:
        return None
    df_clean = df[df['food'] != '-'].copy()
    return df_clean

@st.cache_data
def load_genre_survival_data():
    """요리 장르별 생존율 데이터"""
    df = load_survival_results().copy()
    if df is None or df.empty:
        return None
    df['is_survived'] = df['is_alive'].apply(lambda x: 1 if x in ['생존'] else 0)
    cols = ['round', 'name', 'match_type', 'food_category', 'is_survived', 'is_alive']
    df_analysis = df[cols].copy()
    df_clean = df_analysis.dropna(subset=['food_category'])
    df_clean = df_clean[df_clean['food_category'] != '-']
    return df_clean

@st.cache_data
def load_chef_survival_data():
    """쉐프 생존여부 데이터 로드"""
    file_path = get_data_path('쉐프생존여부.csv')
    if not os.path.exists(file_path):
        return None
    df = pd.read_csv(file_path, encoding='utf-8')
    return df

@st.cache_resource
def get_geojson():
    """GeoJSON 로드"""
    from population_animated_map import load_seoul_geojson
    return load_seoul_geojson()

# === 쉐프 매핑 ===
CHEF_MAPPING = {
    'akrl': '아기맹수', 'choi': '최강록', 'hoo': '후덕죽', 'im': '임성근',
    'jeong': '정호영', 'sam': '샘킴', 'seon': '선재스님', 'son': '손종원',
    'yo': '요리괴물', 'yoon': '윤주모'
}

# === 보조 함수들 ===
def plot_pass_rate(df, judge_col, judge_name):
    """심사위원 합격률 시각화"""
    features = ['how_cook', 'food_category', 'ingrediant', 'temperature']
    fig, axes = plt.subplots(2, 2, figsize=(15, 10))

    for i, col in enumerate(features):
        row, col_idx = divmod(i, 2)
        if col in df.columns:
            pass_rate = df.groupby(col)[judge_col].mean().sort_values(ascending=False)
            sns.barplot(x=pass_rate.index, y=pass_rate.values, ax=axes[row, col_idx], palette='viridis')
            axes[row, col_idx].set_title(f'{col}별 합격률')
            axes[row, col_idx].set_ylim(0, 1.0)
            axes[row, col_idx].tick_params(axis='x', rotation=45)
    plt.tight_layout()
    return fig

def create_pass_rate_summary(df, judge_col):
    """심사위원 합격률 통계표 생성"""
    features = ['how_cook', 'food_category', 'ingrediant', 'temperature']
    feature_names = {
        'how_cook': '조리법',
        'food_category': '음식 카테고리',
        'ingrediant': '주재료',
        'temperature': '온도'
    }

    summary_data = []
    for col in features:
        if col in df.columns:
            stats = df.groupby(col).agg({
                judge_col: ['count', 'sum', 'mean']
            }).reset_index()
            stats.columns = [col, '총 시도 횟수', '합격 횟수', '합격률']
            stats['합격률'] = (stats['합격률'] * 100).round(1)
            stats = stats.sort_values('합격률', ascending=False)
            stats.insert(0, '구분', feature_names[col])
            stats.rename(columns={col: '값'}, inplace=True)
            summary_data.append(stats)

    if summary_data:
        return pd.concat(summary_data, ignore_index=True)
    return pd.DataFrame()

def run_logistic_regression(df, target_col):
    """로지스틱 회귀분석"""
    import statsmodels.api as sm
    if target_col == 'an':
        sub_df = df[df['is_an'] == 1].copy()
    else:
        sub_df = df[df['is_back'] == 1].copy()
        
    features = ['how_cook', 'food_category', 'ingrediant', 'temperature']
    X = pd.get_dummies(sub_df[features], drop_first=True, dtype=int)
    X = sm.add_constant(X)
    y = sub_df[target_col]
    try:
        model = sm.Logit(y, X).fit(disp=0)
        return model, X, y
    except:
        return None, None, None

def calculate_vif(X):
    """다중공선성 계산"""
    from statsmodels.stats.outliers_influence import variance_inflation_factor
    vif_data = pd.DataFrame()
    vif_data["Feature"] = X.columns
    vif_data["VIF"] = [variance_inflation_factor(X.values, i) for i in range(X.shape[1])]
    return vif_data.sort_values(by="VIF", ascending=False)

def create_summary_df(model):
    """회귀분석 결과 요약"""
    if model is None: 
        return pd.DataFrame()
    summary_df = pd.DataFrame({
        "Coef": model.params,
        "P-value": model.pvalues,
        "Odds Ratio": np.exp(model.params)
    })
    return summary_df.sort_values(by="P-value")

@st.cache_data(show_spinner=False)
def load_trend_data():
    """Load the complete, frozen trend snapshot without an API request."""
    df = pd.read_csv(get_data_path('trend_snapshot.csv'))
    df['Date'] = pd.to_datetime(df['Date'])
    return df

# === 메인 화면 ===
def main():
    st.sidebar.title("🍳 흑백요리사 통합 분석")
    st.sidebar.markdown("---")
    
    menu = st.sidebar.radio(
        "분석 메뉴 선택",
        [
            "🏠 홈",
            "📈 쉐프 검색 트렌드 분석",
            "📊 라운드 × 장르별 생존율 분석",
            "🏁 심사위원 합격 예측 분석",
            "📊 방송효과분석"
        ],
        key="main_menu_selection"
    )
    
    st.sidebar.markdown("---")
    st.sidebar.info("흑백요리사 데이터 통합 분석 대시보드")

    # === 홈 ===
    if menu == "🏠 홈":
        st.markdown('<p class="main-header">🍳 흑백요리사 통합 분석 대시보드</p>', unsafe_allow_html=True)
        st.markdown('<p class="sub-header">방송 효과 분석 + 통계 분석 통합 플랫폼</p>', unsafe_allow_html=True)
        
        col1, col2 = st.columns(2)
        with col1:
            st.markdown("""
            ### 📊 방송 효과 분석
            - **리뷰 히트맵**: 방영일별 리뷰 변화 시각화
            - **유동인구 지도**: 서울시 자치구별 일별 유동인구
            - **개별 가게 분석**: 가게별 상세 성과 분석
            """)
        
        with col2:
            st.markdown("""
            ### 📈 통계 분석
            - **심사위원 예측**: 로지스틱 회귀분석 기반
            - **장르별 생존율**: 라운드별 요리 장르 분석
            - **쉐프 트렌드**: Naver/Google/YouTube 통합 트렌드
            """)
        
        st.divider()
        st.markdown("""
        **💡 주요 기능**
        - 📅 방영일 기준 7일 전후 분석
        - 🗺️ 인터랙티브 애니메이션 지도
        - 📊 통계 유의성 기반 인사이트
        - 🎯 심사위원별 합격 공략법
        """)

        changes = load_review_changes()
        episode_delta = changes.groupby('episode')['change_count'].sum()
        st.subheader("한눈에 보는 리뷰 변화")
        st.caption("각 회차 공개일 전후 7일간 출연 가게의 리뷰 건수 차이입니다. 관찰된 변화이며 방송의 인과 효과로 해석할 수 없습니다.")
        cols = st.columns(len(episode_delta))
        for col, (episode, delta) in zip(cols, episode_delta.items()):
            col.metric(f"{episode}회", f"{delta:+,}건")

    # === 쉐프 트렌드 (2번 메뉴) ===
    elif menu == "📈 쉐프 검색 트렌드 분석":
        set_korean_font()
        st.header("📈 쉐프 검색 트렌드 분석")
        st.info("""
        **💡 이 대시보드는?**
        **네이버(초록)**, **구글(파랑)**, **유튜브(빨강)** 3가지 소스를 통합하여 쉐프별 검색 트렌드를 시각화합니다.
        방송 전후로 쉐프들의 인기가 어떻게 변화했는지 확인할 수 있습니다.
        **빨간 점선**은 해당 쉐프의 탈락 시점을 나타냅니다.
        """)

        df_trend = load_trend_data()
        df_survival = load_chef_survival_data()

        if df_trend.empty:
            st.error("트렌드 데이터를 로드할 수 없습니다.")
            return

        # 탈락 정보 파싱
        elimination_info = {}

        # 수동 매핑 (CSV 이름 -> 그래프 쉐프명)
        name_mapping = {
            '아기맹수': '아기맹수',
            '샘 킴': '샘킴',
            '샘킴': '샘킴',
            '손종원': '손종원',
            '선재스님': '선재스님',
            '임성근': '임성근',
            '정호영': '정호영',
            '후덕죽': '후덕죽',
            '술 빚는 윤주모': '윤주모',
            '윤주모': '윤주모',
            '윤준모': '윤주모',
            '이하성 (요리괴물)': '요리괴물',
            '요리괴물': '요리괴물',
            '최강록': '최강록'
        }

        if df_survival is not None:
            for _, row in df_survival.iterrows():
                if pd.notna(row['탈락자 (Eliminated)']) and row['탈락자 (Eliminated)'].strip():
                    eliminated = [name.strip() for name in row['탈락자 (Eliminated)'].split(',')]
                    elim_date = pd.to_datetime(row['공개일'].replace('.', '-'))
                    for chef_name in eliminated:
                        # 수동 매핑 사용
                        mapped_name = name_mapping.get(chef_name, chef_name)
                        elimination_info[mapped_name] = elim_date

        # 요리괴물(준우승) 수동 추가
        elimination_info['요리괴물'] = pd.to_datetime('2026-01-13')

        st.subheader("⚙️ 필터 설정")
        col1, col2 = st.columns(2)
        with col1:
            all_chefs = sorted(df_trend['Chef'].unique())
            selected_chefs = st.multiselect("쉐프 선택", options=all_chefs, default=all_chefs[:3])
        with col2:
            all_sources = ['datalab', 'google', 'youtube']
            selected_sources = st.multiselect("소스 선택", options=all_sources, default=all_sources)

        plot_df = df_trend.copy()
        if selected_chefs:
            plot_df = plot_df[plot_df['Chef'].isin(selected_chefs)]
        if selected_sources:
            plot_df = plot_df[plot_df['Source'].isin(selected_sources)]


        # 소스명 변경 (datalab -> Naver)
        plot_df['Source'] = plot_df['Source'].replace('datalab', 'Naver')

        if not plot_df.empty:
            color_palette = {'google': 'blue', 'Naver': 'green', 'youtube': 'red'}

            fig = sns.relplot(
                data=plot_df, x="Date", y="Value", hue="Source", col="Chef",
                kind="line", palette=color_palette,
                col_wrap=3, height=4, aspect=1.5,
                facet_kws={'sharey': False, 'sharex': True},
                errorbar=None  # 오차범위(그림자) 제거 (ci=None deprecated in new seaborn)
            )

            # 각 쉐프별로 탈락 시점 및 기간 표시
            for ax in fig.axes.flat:
                chef_title = ax.get_title().replace('Chef = ', '')
                ax.set_title(f'Chef = {chef_title}')

                if chef_title in elimination_info:
                    elim_date = elimination_info[chef_title]
                    # 탈락 시점이 표시 범위 내에 있을 때만 표시
                    if pd.Timestamp('2025-12-09') <= elim_date <= pd.Timestamp('2026-01-20'):
                        ax.axvline(x=elim_date, color='red', linestyle='--', linewidth=2, alpha=0.7)
                        y_max = ax.get_ylim()[1]
                        ax.text(elim_date, y_max * 0.95, '탈락', rotation=0,
                               horizontalalignment='right', verticalalignment='top', color='red', fontsize=9, fontweight='bold')
                ax.tick_params(axis='x', rotation=45)

            st.pyplot(fig.fig)
            plt.close(fig.fig)
            st.caption("구글·유튜브는 셰프·날짜별 3개 관측값의 평균, 네이버는 1개 관측값입니다. 플랫폼별 지수는 기준이 달라 절대값을 직접 비교하지 마세요.")

            st.markdown("""
            **🎨 색상 가이드:**
            - 🟢 **Naver**: 네이버 데이터랩 검색량
            - 🔵 **google**: 구글 트렌드
            - 🔴 **youtube**: 유튜브 검색량
            - 🔴 **빨간 점선**: 해당 쉐프 탈락 시점
            """)

            # 쉐프 생존여부 테이블 추가
            st.divider()
            st.subheader("📋 쉐프별 탈락 정보")

            if df_survival is not None:
                # 탈락자가 있는 행만 필터링
                elimination_rows = df_survival[df_survival['탈락자 (Eliminated)'].notna() &
                                              (df_survival['탈락자 (Eliminated)'].str.strip() != '')]

                if not elimination_rows.empty:
                    display_df = elimination_rows[['라운드', '공개일', '진행 내용 (줄거리)', '탈락자 (Eliminated)']].copy()
                    display_df.columns = ['라운드', '공개일', '진행 내용', '탈락자']
                    st.dataframe(display_df, use_container_width=True, hide_index=True)
                else:
                    st.info("탈락자 정보가 없습니다.")
            else:
                st.warning("쉐프생존여부.csv 파일을 찾을 수 없습니다.")

    # === 장르별 생존율 (3번 메뉴) ===
    elif menu == "📊 라운드 × 장르별 생존율 분석":
        set_korean_font()
        st.header("📊 요리 장르별 생존율 분석")
        st.info("""
        **💡 이 대시보드는?**
        TV 서바이벌 요리 프로그램의 참가자 데이터를 바탕으로 **요리 장르(Food Category)가 생존에 미치는 영향**을 통계적으로 분석합니다.
        라운드별, 경기 유형별로 어떤 장르가 유리한지 확인할 수 있습니다.
        """)

        df_clean = load_genre_survival_data()
        if df_clean is None:
            st.error("데이터를 찾을 수 없습니다.")
            return

        genre_view = st.selectbox(
            "분석 내용 선택",
            ["📈 라운드별", "🏆 최고 생존율", "⚔️ 경기 유형", "📝 결론"],
            key="genre_view",
        )

        if genre_view == "📈 라운드별":
            st.subheader("라운드 × 장르별 생존율 분석")
            st.markdown("""
            각 라운드에서 어떤 요리 장르가 강세를 보였는지 확인하기 위해 교차 분석표를 생성합니다.
            - **분모**: 해당 라운드 & 장르의 총 참가자 수
            - **분자**: 해당 라운드 & 장르의 생존자 수
            """)

            survival_rates = df_clean.groupby(['round', 'food_category'])['is_survived'].agg(['count', 'sum', 'mean']).reset_index()
            survival_rates.columns = ['round', 'food_category', 'participants', 'survivors', 'survival_rate']
            survival_rates['survival_rate_pct'] = survival_rates['survival_rate'] * 100

            pivot_survival = survival_rates.pivot_table(index='round', columns='food_category', values='survival_rate_pct')

            fig, ax = plt.subplots(figsize=(12, 8))
            sns.heatmap(pivot_survival, annot=True, fmt='.1f', cmap='RdYlGn', vmin=0, vmax=100, ax=ax)
            ax.set_title('라운드별 요리 장르 생존율 (%)', fontsize=14)
            ax.set_ylabel('라운드')
            ax.set_xlabel('요리 장르')
            st.pyplot(fig)
            st.caption("🔎 **그래프 보는 법**: 초록색이 짙을수록 생존율이 높습니다. 빨간색에 가까울수록 생존율이 낮습니다.")

        elif genre_view == "🏆 최고 생존율":
            st.subheader("라운드별 최고 생존율 장르")
            st.markdown("""
            각 라운드에서 **가장 높은 생존율**을 기록한 요리 장르를 요약합니다.

            ⚠️ **주의**: 참가자 수가 극히 적은 경우(예: 1명) 생존율 100%나 0%가 나올 수 있음에 유의해야 합니다.
            """)

            survival_rates = df_clean.groupby(['round', 'food_category'])['is_survived'].agg(['count', 'sum', 'mean']).reset_index()
            survival_rates.columns = ['round', 'food_category', 'participants', 'survivors', 'survival_rate']

            best_performers = []
            for r in survival_rates['round'].unique():
                round_data = survival_rates[survival_rates['round'] == r]
                max_rate = round_data['survival_rate'].max()
                best_genres = round_data[round_data['survival_rate'] == max_rate]
                best_performers.append({
                    'Round': r,
                    'Best Genre': ", ".join(best_genres['food_category'].tolist()),
                    'Survival Rate (%)': round(max_rate * 100, 2),
                    'Participants': ", ".join(best_genres['participants'].astype(str).tolist())
                })

            df_best = pd.DataFrame(best_performers)
            st.dataframe(df_best, use_container_width=True)

        elif genre_view == "⚔️ 경기 유형":
            st.subheader("경기 유형(팀전 vs 개인전)별 분석")
            st.markdown("""
            '개인전'과 '팀전'에서 특정 요리 장르가 더 유리하게 작용하는지 분석합니다.
            """)

            match_type_stats = df_clean.groupby(['match_type', 'food_category'])['is_survived'].agg(['count', 'mean']).reset_index()
            match_type_stats['survival_rate_pct'] = match_type_stats['mean'] * 100

            fig, ax = plt.subplots(figsize=(12, 6))
            sns.barplot(data=match_type_stats, x='food_category', y='survival_rate_pct', hue='match_type', ax=ax)
            ax.set_title('경기 유형별 요리 장르 생존율', fontsize=14)
            ax.set_ylabel('생존율 (%)')
            ax.set_xlabel('요리 장르')
            ax.legend(title='경기 유형')
            ax.set_ylim(0, 110)

            for p in ax.patches:
                height = p.get_height()
                if height > 0:
                    ax.text(p.get_x() + p.get_width()/2., height + 1, f'{int(height)}%', ha='center')

            st.pyplot(fig)
            st.caption("🔎 **그래프 보는 법**: 막대 높이가 높을수록 해당 장르의 생존율이 높습니다.")

        else:
            st.markdown("""
            ### 🔍 주요 인사이트

            **✅ 장르의 유불리**
            - 특정 라운드 미션에 따라 장르별 유리함이 달라집니다.
            - 3-1R(재료 대결), 5-2R(무한 요리 지옥)에서는 창의성이 요구되는 **퓨전음식**이 유리

            **✅ 전략 제안**
            - 초반: 본인의 주력 장르(정통성)로 어필
            - 중반 이후: 팀전/변수 미션에 **퓨전/창의적 접근** 가미
            """)

    # === 심사위원 예측 (4번 메뉴) ===
    elif menu == "🏁 심사위원 합격 예측 분석":
        set_korean_font()
        st.header("🏁 심사위원 합격 예측 분석")
        st.info("""
        **💡 이 대시보드는?**
        **백종원**, **안성재** 두 심사위원의 심사 성향과 합격 기준을 통계적 기법(로지스틱 회귀분석)으로 분석합니다.
        어떤 조리법과 재료가 합격 확률을 높이는지 데이터로 확인할 수 있습니다.
        """)

        df = load_survival_data()
        if df is None:
            st.error("데이터를 찾을 수 없습니다.")
            return

        judge_view = st.selectbox(
            "분석 내용 선택",
            ["📊 EDA", "📈 회귀분석", "💡 공략법"],
            key="judge_view",
        )

        if judge_view == "📊 EDA":
            col1, col2 = st.columns(2)
            with col1:
                st.subheader("안성재 심사위원")
                fig_an = plot_pass_rate(df[df['is_an'] == 1], 'an', '안성재')
                st.pyplot(fig_an)
                st.caption("📝 **해석**: 각 막대의 높이는 합격률을 의미합니다. 안성재 심사위원은 특정 조리법(조림 등)에서 확연히 높은 합격률을 보이는 경향이 있습니다.")

                # 안성재 통계표
                with st.expander("📊 상세 통계표 (시도 횟수 포함)"):
                    summary_an = create_pass_rate_summary(df[df['is_an'] == 1], 'an')
                    if not summary_an.empty:
                        st.dataframe(summary_an, hide_index=True, use_container_width=True)
                    else:
                        st.info("통계 데이터가 없습니다.")

            with col2:
                st.subheader("백종원 심사위원")
                fig_back = plot_pass_rate(df[df['is_back'] == 1], 'back', '백종원')
                st.pyplot(fig_back)
                st.caption("📝 **해석**: 백종원 심사위원은 퓨전 및 다양한 조리법에서 상대적으로 고른 합격률을 보이지만, 특정 '맛'의 포인트(예: 중식 튀김)를 선호함을 알 수 있습니다.")

                # 백종원 통계표
                with st.expander("📊 상세 통계표 (시도 횟수 포함)"):
                    summary_back = create_pass_rate_summary(df[df['is_back'] == 1], 'back')
                    if not summary_back.empty:
                        st.dataframe(summary_back, hide_index=True, use_container_width=True)
                    else:
                        st.info("통계 데이터가 없습니다.")

            st.divider()
            st.warning("""
            ⚠️ **합격률 해석 시 주의사항**

            합격률이 1.0(100%)으로 표시되는 경우는 해당 조건의 데이터가 1개뿐인데 그것이 합격이라서 합격률이 100%가 된 것입니다.
            예를 들어, '5-2R 당근케이크'처럼 특정 조리법이나 재료 조합의 데이터가 극히 적은 경우(1~2개),
            통계적 유의미함이 떨어지므로 **샘플 수가 충분한 조건**을 위주로 해석해야 합니다.

            💡 **Tip**: 아래 '📊 상세 통계표'를 열어보면 각 조건별 **총 시도 횟수**와 **합격 횟수**를 확인할 수 있습니다.
            """)

        elif judge_view == "📈 회귀분석":
            st.warning("⚠️ **주의사항**: 표본이 적어 회귀 진단이 맞지않기에 정확한 모델링이 아니며 재미로 보길 바랍니다.")
            col_l, col_r = st.columns(2)

            model_an, X_an, y_an = run_logistic_regression(df, 'an')
            summary_an = create_summary_df(model_an)
            with col_l:
                st.subheader("🔹 안성재 심사위원 모델")
                st.markdown("##### 📋 통계 분석 결과표")
                st.dataframe(summary_an.style.map(lambda x: 'background-color: yellow' if x < 0.05 else '', subset=['P-value']), height=400)
                st.info("""💡 **결과 해석 가이드**:
- **P-value (노란색)**: 0.05 미만이면 결과가 통계적으로 매우 유의미함을 뜻합니다.
- **Odds Ratio**: 1보다 크면 합격 확률을 **높이는** 요인, 1보다 작으면 **낮추는** 요인입니다.""")

                if X_an is not None:
                    with st.expander("다중공선성(VIF) 진단"):
                        vif_an = calculate_vif(X_an)
                        st.dataframe(vif_an.style.map(lambda x: 'color: red' if x > 10 else '', subset=['VIF']))
                        st.caption("🔎 **VIF란?**: 변수들 간의 상관관계입니다. 10 이상(빨간색)이면 신뢰도가 떨어질 수 있습니다.")

                if model_an:
                    st.markdown("##### 📉 잔차(오차) 분석")
                    fig_res, ax = plt.subplots(figsize=(8, 4))
                    # Use numpy arrays to prevent index alignment issues with seaborn regplot lowess
                    sns.regplot(x=np.array(model_an.predict()), y=np.array(model_an.resid_pearson), lowess=True,
                                line_kws={'color': 'red'}, scatter_kws={'alpha': 0.5}, ax=ax)
                    ax.set_title("Residuals vs Fitted (안성재)")
                    ax.axhline(0, color='blue', linestyle='--')
                    ax.set_ylim(-4, 4)  # 극단적 이상치 시각화 방지
                    st.pyplot(fig_res)
                    st.caption("🔎 **그래프 보는 법**: 빨간 실선(데이터 추세)이 파란 점선(0)에 가깝고 평평할수록, 모델이 데이터를 편향 없이 잘 설명하고 있다는 뜻입니다.")

            model_back, X_back, y_back = run_logistic_regression(df, 'back')
            summary_back = create_summary_df(model_back)
            with col_r:
                st.subheader("🔸 백종원 심사위원 모델")
                st.markdown("##### 📋 통계 분석 결과표")
                st.dataframe(summary_back.style.map(lambda x: 'background-color: yellow' if x < 0.05 else '', subset=['P-value']), height=400)
                st.info("""💡 **결과 해석 가이드**:
- **P-value (노란색)**: 이 값이 작을수록 해당 변수가 합격/불합격에 미치는 영향이 확실합니다.
- **Odds Ratio**: 숫자가 클수록 해당 요리를 했을 때 합격할 확률이 압도적으로 높아집니다.""")

                if X_back is not None:
                    with st.expander("다중공선성(VIF) 진단"):
                        vif_back = calculate_vif(X_back)
                        st.dataframe(vif_back.style.map(lambda x: 'color: red' if x > 10 else '', subset=['VIF']))
                        st.caption("🔎 **VIF란?**: 10 이하가 이상적입니다. 너무 높으면 '같은 의미의 변수'가 여러 개 들어갔다는 뜻입니다.")

                if model_back:
                    st.markdown("##### 📉 잔차(오차) 분석")
                    fig_res_b, ax_b = plt.subplots(figsize=(8, 4))
                    # Use numpy arrays to prevent index alignment issues
                    sns.regplot(x=np.array(model_back.predict()), y=np.array(model_back.resid_pearson), lowess=True,
                                line_kws={'color': 'red'}, scatter_kws={'alpha': 0.5}, ax=ax_b)
                    ax_b.set_title("Residuals vs Fitted (백종원)")
                    ax_b.axhline(0, color='blue', linestyle='--')
                    ax_b.set_ylim(-4, 4)  # 극단적 이상치 시각화 방지
                    st.pyplot(fig_res_b)
                    st.caption("🔎 **그래프 보는 법**: 데이터들(점들)이 위아래로 고르게 퍼져 있어야 좋은 모델입니다. 특정 패턴이 보이면 모델 개선이 필요할 수 있습니다.")

        else:
            col1, col2 = st.columns(2)
            with col1:
                st.markdown("### 🤵 안성재 심사위원")
                st.success("✅ **필승**: 조림(Braising)")
                st.error("❌ **필패**: 해산물")
                st.info("💡 기본에 충실한 '조림'으로 깊은 맛을 어필하세요.")
            with col2:
                st.markdown("### 👨‍🍳 백종원 심사위원")
                st.success("✅ **필승**: 튀김, 중식")
                st.warning("⚠️ **선호**: 중식 스타일")
                st.info("💡 '조림' 또는 '중식/튀김'으로 승부하세요.")

    # === 방송 효과 분석 (5번 메뉴) ===
    elif menu == "📊 방송효과분석":
        st.markdown('<p class="main-header">📊 흑백요리사 방송 효과 분석</p>', unsafe_allow_html=True)
        st.markdown('<p class="sub-header">방영일 기준 7일 전후 리뷰 및 유동인구 변화</p>', unsafe_allow_html=True)

        # 탭 선택 (selectbox 방식으로 변경 - Streamlit Cloud 호환성 개선)
        broadcast_tab_selection = st.selectbox(
            "분석 내용 선택",
            options=["📊 리뷰 히트맵", "🗺️ 유동인구 지도", "📈 가게 분석"],
            key="broadcast_tab_selection",
            label_visibility="collapsed"
        )

        st.divider()

        if broadcast_tab_selection == "📊 리뷰 히트맵":
            from review_heatmap import create_review_heatmap, get_top_restaurants_by_change
            review_changes = load_review_changes()
            restaurants = load_restaurant_details()
            st.header("📊 방영일별 리뷰 변화")
            st.info("""
            **💡 이 대시보드는?**
            흑백요리사 출연 가게들의 **리뷰 변화**를 분석합니다. 각 방영일 기준으로 7일 전과 후를 비교하여
            리뷰 증가율과 증가 수를 시각화했습니다. 색이 진할수록 리뷰 증가가 많았던 가게입니다.
            """)

            # 필터: 방영 회차 선택
            episode_labels = {
                1: "1회 (12/16)", 2: "2회 (12/23)", 3: "3회 (12/30)",
                4: "4회 (1/6)", 5: "5회 (1/13)"
            }
            selected_episode_tab1 = st.selectbox(
                "방영 회차 선택 (TOP 10용)",
                options=list(episode_labels.keys()),
                format_func=lambda x: episode_labels[x],
                index=0,
                key="episode_tab1"
            )

            episode_changes = review_changes[review_changes['episode'] == selected_episode_tab1]
            valid_rates = episode_changes.loc[episode_changes['before_count'] > 0, 'change_rate']

            col1, col2, col3 = st.columns(3)
            with col1:
                st.metric("분석 가게", episode_changes['restaurant'].nunique())
            with col2:
                st.metric("리뷰 건수 변화", f"{episode_changes['change_count'].sum():+,}건")
            with col3:
                st.metric("증가율 중앙값", f"{valid_rates.median():+.1f}%")
            st.caption(f"선택한 회차의 전후 7일 비교입니다. 증가율 중앙값은 방영 전 리뷰가 1건 이상인 가게 {len(valid_rates)}곳 기준입니다.")

            st.divider()

            value_option = st.radio(
                "표시 값",
                options=['change_rate', 'change_count'],
                format_func=lambda x: '증가율 (%)' if x == 'change_rate' else '증가 수',
                horizontal=True,
                key="value_option_tab1"
            )

            # 계산 공식 설명
            if value_option == 'change_rate':
                st.caption("📐 **계산 공식**: (방영 후 리뷰 수 - 방영 전 리뷰 수) ÷ 방영 전 리뷰 수 × 100. 방영 전 0건은 증가율을 계산할 수 없어 빈칸으로 표시합니다.")
            else:
                st.caption("📐 **계산 공식**: 방영 후 리뷰 수 - 방영 전 리뷰 수 → 실제로 늘어난 리뷰 개수를 보여줍니다")

            fig_heatmap = create_review_heatmap(review_changes, restaurants, value_column=value_option, min_reviews=0, max_restaurants=30)
            st.plotly_chart(fig_heatmap, use_container_width=True)
            st.caption("가독성을 위해 전체 102곳 중 분석 기간 리뷰 수 상위 30곳을 표시합니다. 모든 가게는 '가게 분석'에서 확인할 수 있습니다.")

            st.subheader("🏆 리뷰 증가율 TOP 10")
            top10 = get_top_restaurants_by_change(review_changes, episode=selected_episode_tab1, top_n=10)
            st.dataframe(
                top10[['restaurant', 'change_rate', 'before_count', 'after_count']].rename(columns={
                    'restaurant': '가게명', 'change_rate': '증가율 (%)',
                    'before_count': '방영 전', 'after_count': '방영 후'
                }),
                hide_index=True
            )

        elif broadcast_tab_selection == "🗺️ 유동인구 지도":
            from population_animated_map import (
                create_animated_population_map,
                create_broadcast_comparison_map,
                create_static_choropleth,
            )
            daily_pop = load_daily_population()
            restaurants = load_restaurant_details()
            geojson = get_geojson()
            st.header("🗺️ 서울시 유동인구 변화 지도")
            st.info("""
            **💡 이 대시보드는?**
            서울시 자치구별 **유동인구 변화**를 지도 위에 표시합니다.
            - 🎬 **애니메이션**: 날짜별로 유동인구가 어떻게 변화했는지 확인
            - 📊 **변화율 지도**: 방영 전후 유동인구 증감률 비교 (빨강=증가, 파랑=감소)
            - ★ **회색 마커**: 흑백요리사 출연 가게 위치
            """)
            coverage = daily_pop.groupby('date')['district'].nunique()
            st.caption(f"자료 범위: {daily_pop['date'].min():%Y-%m-%d} ~ {daily_pop['date'].max():%Y-%m-%d}. 일별 자치구 관측은 {coverage.min()}~{coverage.max()}곳/25곳이며, 센서 관측 수가 달라 단순 합계의 전후 비교에는 주의가 필요합니다.")

            map_type = st.radio(
                "지도 유형",
                options=['animation', 'comparison', 'static'],
                format_func=lambda x: {
                    'animation': '🎬 애니메이션 지도',
                    'comparison': '📊 변화율 지도',
                    'static': '📍 특정 날짜'
                }[x],
                horizontal=True,
                index=1,
                key="map_type_tab2"
            )

            if map_type == 'animation':
                st.info("▶ 재생 버튼을 눌러 일별 유동인구 변화를 확인하세요.")

                # 애니메이션용 날짜 범위 필터
                all_dates = sorted(daily_pop['date'].unique())
                date_range_tab2 = st.date_input(
                    "분석 기간",
                    value=(all_dates[0], all_dates[min(13, len(all_dates) - 1)]),
                    min_value=all_dates[0],
                    max_value=all_dates[-1],
                    key="date_range_tab2"
                )

                with st.spinner("애니메이션 지도 생성 중..."):
                    start_str = date_range_tab2[0].strftime('%Y-%m-%d') if isinstance(date_range_tab2, tuple) else str(date_range_tab2[0])
                    end_str = date_range_tab2[1].strftime('%Y-%m-%d') if isinstance(date_range_tab2, tuple) and len(date_range_tab2) > 1 else str(date_range_tab2[-1])
                    fig_map = create_animated_population_map(daily_pop, restaurants, geojson, start_date=start_str, end_date=end_str)
                    st.plotly_chart(fig_map, use_container_width=True)

            elif map_type == 'comparison':
                # 방영 회차 선택
                episode_labels_tab2 = {
                    1: "1회 (12/16)", 2: "2회 (12/23)", 3: "3회 (12/30)",
                    4: "4회 (1/6)", 5: "5회 (1/13)"
                }
                selected_episode_tab2 = st.selectbox(
                    "방영 회차 선택",
                    options=list(episode_labels_tab2.keys()),
                    format_func=lambda x: episode_labels_tab2[x],
                    index=0,
                    key="episode_tab2"
                )

                broadcast_date = BROADCAST_DATES[selected_episode_tab2 - 1]
                st.info(f"📊 방영일 {broadcast_date} 기준 7일 전후 변화율")
                fig_comp = create_broadcast_comparison_map(daily_pop, restaurants, broadcast_date, geojson)
                st.plotly_chart(fig_comp, use_container_width=True)

            else:
                # 특정 날짜 선택
                selected_date_tab2 = st.date_input(
                    "날짜 선택",
                    value=pd.to_datetime(BROADCAST_DATES[0]),
                    key="date_tab2"
                )
                fig_static = create_static_choropleth(daily_pop, restaurants, str(selected_date_tab2), geojson)
                st.plotly_chart(fig_static, use_container_width=True)
            
            st.subheader("★ 흑백요리사 출연 가게")
            rest_display = restaurants[['restaurant', 'chief_info', 'category', 'location', 'review_count']].copy()
            rest_display.columns = ['가게명', '셰프', '카테고리', '위치', '리뷰수']
            st.dataframe(rest_display, hide_index=True)

        elif broadcast_tab_selection == "📈 가게 분석":
            from review_heatmap import create_review_bar_chart
            review_changes = load_review_changes()
            restaurants = load_restaurant_details()
            st.header("📈 개별 가게 분석")
            st.info("""
            **💡 이 대시보드는?**
            특정 가게를 선택하여 **회차별 상세 데이터**를 확인합니다.
            각 방영일마다 리뷰가 얼마나 증가했는지 막대그래프와 표로 확인할 수 있습니다.
            """)

            all_restaurants = sorted(review_changes['restaurant'].unique())
            selected_restaurant = st.selectbox("가게 선택", options=all_restaurants, key="restaurant_tab3")
            
            if selected_restaurant:
                rest_info = restaurants[restaurants['restaurant'] == selected_restaurant]
                if len(rest_info) > 0:
                    info = rest_info.iloc[0]
                    col1, col2, col3, col4 = st.columns(4)
                    with col1:
                        st.metric("셰프", info.get('chief_info', 'N/A'))
                    with col2:
                        st.metric("카테고리", info.get('category', 'N/A'))
                    with col3:
                        st.metric("위치", info.get('location', 'N/A'))
                    with col4:
                        st.metric("총 리뷰", info.get('review_count', 'N/A'))
                
                fig_bar = create_review_bar_chart(review_changes, selected_restaurant)
                st.plotly_chart(fig_bar, use_container_width=True)
                
                st.subheader("📋 회차별 상세 데이터")
                rest_changes = review_changes[review_changes['restaurant'] == selected_restaurant]
                display_df = rest_changes[['episode', 'broadcast_date', 'before_count', 'after_count', 'change_count', 'change_rate']]
                display_df.columns = ['회차', '방영일', '방영 전', '방영 후', '증가 수', '증가율 (%)']
                st.dataframe(display_df, hide_index=True)


    st.divider()
    st.markdown("""
    <div style="text-align: center; color: #888; font-size: 0.9rem;">
        흑백요리사 통합 분석 대시보드 v2.0<br>
        데이터 출처: 캐치테이블, 서울시 유동인구, 네이버/구글/유튜브<br>
        분석 기간: 2025-12-09 ~ 2026-01-20
    </div>
    """, unsafe_allow_html=True)

if __name__ == '__main__':
    main()
