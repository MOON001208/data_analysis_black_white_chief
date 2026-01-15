
import streamlit as st
import pandas as pd
import numpy as np
import statsmodels.api as sm
import matplotlib.pyplot as plt
import seaborn as sns
from matplotlib import font_manager, rc
import os
import glob
import platform
from statsmodels.stats.outliers_influence import variance_inflation_factor

# --- 1. Page Config (Must be first) ---
st.set_page_config(
    page_title="흑백요리사 통합 분석 대시보드 (Ver.2)",
    page_icon="🍳",
    layout="wide"
)

# --- 2. Shared Utilities (Fonts) ---
def set_korean_font():
    system_name = platform.system()
    if system_name == "Windows":
        font_path = "c:/Windows/Fonts/malgun.ttf"
        try:
            font_name = font_manager.FontProperties(fname=font_path).get_name()
            rc('font', family=font_name)
        except:
            plt.rcParams['font.family'] = 'Malgun Gothic'
    elif system_name == "Darwin":
        rc('font', family="AppleGothic")
    else:
        plt.rcParams['font.family'] = 'sans-serif'
    plt.rcParams['axes.unicode_minus'] = False

set_korean_font()

# --- 3. Page 1 Logic: Survival Prediction Report (Ver.2 Data) ---
def show_survival_analysis():
    st.header("🏁 심사위원 합격 예측 분석 (Ver.2)")
    st.markdown("""
    본 분석은 **'셰프서바이벌결과요약.csv'** 데이터를 바탕으로, 
    두 심사위원(**백종원**, **안성재**)의 심사 성향과 합격 기준을 통계적 기법(로지스틱 회귀분석)으로 분석한 결과입니다.
    """)

    # --- Data Loading ---
    @st.cache_data
    def load_survival_data():
        # Changed to new file
        file_path = '셰프서바이벌결과요약.csv'
        if not os.path.exists(file_path):
            return None
        df = pd.read_csv(file_path)
        df_clean = df[df['food'] != '-'].copy()
        return df_clean

    df = load_survival_data()

    if df is None:
        st.error("데이터 파일('셰프서바이벌결과요약.csv')을 찾을 수 없습니다.")
        return

    # --- Helper Functions ---
    def plot_pass_rate(df, judge_col, judge_name):
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

    def run_logistic_regression(df, target_col):
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
        vif_data = pd.DataFrame()
        vif_data["Feature"] = X.columns
        vif_data["VIF"] = [variance_inflation_factor(X.values, i) for i in range(X.shape[1])]
        return vif_data.sort_values(by="VIF", ascending=False)

    def create_summary_df(model):
        if model is None: return pd.DataFrame()
        summary_df = pd.DataFrame({
            "Coef": model.params,
            "P-value": model.pvalues,
            "Odds Ratio": np.exp(model.params)
        })
        return summary_df.sort_values(by="P-value")

    # --- Content Layout ---
    tab1, tab2, tab3 = st.tabs(["📊 데이터 탐색 (EDA)", "📈 회귀분석 결과", "💡 공략 리포트"])

    with tab1:
        col1, col2 = st.columns(2)
        with col1:
            st.subheader("안성재 심사위원")
            fig_an = plot_pass_rate(df[df['is_an'] == 1], 'an', '안성재')
            st.pyplot(fig_an)
            st.caption("📝 **해석**: 각 막대의 높이는 합격률을 의미합니다. 안성재 심사위원은 특정 조리법(조림 등)에서 확연히 높은 합격률을 보이는 경향이 있습니다.")

        with col2:
            st.subheader("백종원 심사위원")
            fig_back = plot_pass_rate(df[df['is_back'] == 1], 'back', '백종원')
            st.pyplot(fig_back)
            st.caption("📝 **해석**: 백종원 심사위원은 퓨전 및 다양한 조리법에서 상대적으로 고른 합격률을 보이지만, 특정 '맛'의 포인트(예: 중식 튀김)를 선호함을 알 수 있습니다.")

    with tab2:
        col_l, col_r = st.columns(2)
        
        # Ahn Analysis
        model_an, X_an, y_an = run_logistic_regression(df, 'an')
        summary_an = create_summary_df(model_an)
        with col_l:
            st.subheader("🔹 안성재 심사위원 모델")
            st.markdown("##### 📋 통계 분석 결과표")
            st.dataframe(summary_an.style.map(lambda x: 'background-color: yellow' if x < 0.05 else '', subset=['P-value']), height=400)
            st.info("""
            💡 **결과 해석 가이드**:
            - **P-value (노란색)**: 0.05 미만이면 결과가 통계적으로 매우 유의미함을 뜻합니다.
            - **Odds Ratio**: 1보다 크면 합격 확률을 **높이는** 요인, 1보다 작으면 **낮추는** 요인입니다.
            """)
            
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
                ax.axhline(0, color='blue', linestyle='--') # Blue dashed line
                st.pyplot(fig_res)
                st.caption("🔎 **그래프 보는 법**: 빨간 실선(데이터 추세)이 파란 점선(0)에 가깝고 평평할수록, 모델이 데이터를 편향 없이 잘 설명하고 있다는 뜻입니다.")

        # Baek Analysis
        model_back, X_back, y_back = run_logistic_regression(df, 'back')
        summary_back = create_summary_df(model_back)
        with col_r:
            st.subheader("🔸 백종원 심사위원 모델")
            st.markdown("##### 📋 통계 분석 결과표")
            st.dataframe(summary_back.style.map(lambda x: 'background-color: yellow' if x < 0.05 else '', subset=['P-value']), height=400)
            st.info("""
            💡 **결과 해석 가이드**:
            - **P-value (노란색)**: 이 값이 작을수록 해당 변수가 합격/불합격에 미치는 영향이 확실합니다.
            - **Odds Ratio**: 숫자가 클수록 해당 요리를 했을 때 합격할 확률이 압도적으로 높아집니다.
            """)
            
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
                ax_b.axhline(0, color='blue', linestyle='--') # Blue dashed line
                st.pyplot(fig_res_b)
                st.caption("🔎 **그래프 보는 법**: 데이터들(점들)이 위아래로 고르게 퍼져 있어야 좋은 모델입니다. 특정 패턴이 보이면 모델 개선이 필요할 수 있습니다.")

    with tab3:
        col_final_1, col_final_2 = st.columns(2)
        with col_final_1:
            st.markdown("### 🤵 안성재 심사위원 (Ver.2)")
            st.success("**✅ 필승 전략**: 조림(Braising)")
            st.error("**❌ 필패 전략**: 튀김(Frying)")
            st.info("💡 **전략**: 기본에 충실한 '조림'으로 깊은 맛을 어필하세요. 튀김은 피하는 것이 좋습니다.")
        with col_final_2:
            st.markdown("### 👨‍🍳 백종원 심사위원 (Ver.2)")
            st.success("**✅ 필승 전략**: 퓨전(Fusion), 튀김(Frying)")
            st.warning("**⚠️ 참고**: 중식 스타일 선호")
            st.info("💡 **전략**: 창의적인 '퓨전' 메뉴나 강력한 화력의 '튀김/볶음' 요리로 승부하세요.")
        
        st.divider()
        st.markdown("### 📝 심사위원 비교 (Ver.2)")
        comparison_data = {
            "항목": ["선호 조리법", "비선호 조리법", "핵심 키워드"],
            "안성재 (Ahn)": ["조림", "튀김", "#깊은맛 #기본기 #조림"],
            "백종원 (Baek)": ["퓨전, 튀김", "평범한 한식", "#창의성 #퓨전 #직관적맛"]
        }
        st.table(pd.DataFrame(comparison_data).set_index("항목"))


# --- 4. Page 2 Logic: Trend Analysis Report (Keep as is) ---
def show_trend_analysis():
    st.header("📈 쉐프 검색 트렌드 분석 (Naver vs Google)")
    
    # --- Data Loading ---
    @st.cache_data
    def load_trend_data():
        base_path = r"c:\Users\USER\Documents\웅진씽크빅kdt\흑백요리사\흑백요리사트렌드추이"
        if not os.path.exists(base_path):
            st.error(f"데이터 경로를 찾을 수 없습니다: {base_path}")
            return pd.DataFrame()

        datalab_files = glob.glob(os.path.join(base_path, "*_datalab.csv"))
        all_data = []

        for f_naver in datalab_files:
            try:
                filename = os.path.basename(f_naver)
                chef_id = filename.replace("_datalab.csv", "")
                f_google = os.path.join(base_path, f"{chef_id}_google.csv")
                
                if not os.path.exists(f_google): continue
                    
                try: df_naver = pd.read_csv(f_naver, encoding='utf-8')
                except: df_naver = pd.read_csv(f_naver, encoding='cp949')
                if df_naver.shape[1] < 2: continue
                chef_name = df_naver.columns[1]
                df_naver = df_naver.rename(columns={df_naver.columns[0]: 'Date', df_naver.columns[1]: 'Value'})
                df_naver['Source'] = 'Naver'
                df_naver['Chef'] = chef_name
                
                try: df_google = pd.read_csv(f_google, encoding='utf-8')
                except: df_google = pd.read_csv(f_google, encoding='cp949')
                if df_google.shape[1] < 2: continue
                df_google = df_google.rename(columns={df_google.columns[0]: 'Date', df_google.columns[1]: 'Value'})
                df_google['Source'] = 'Google'
                df_google['Chef'] = chef_name
                
                df_naver = df_naver.dropna(subset=['Value'])
                df_google = df_google.dropna(subset=['Value'])
                df_naver['Value'] = pd.to_numeric(df_naver['Value'], errors='coerce')
                df_google['Value'] = pd.to_numeric(df_google['Value'], errors='coerce')
                
                all_data.extend([df_naver, df_google])
            except Exception as e:
                print(f"Error: {e}")

        if not all_data: return pd.DataFrame()
        final_df = pd.concat(all_data, ignore_index=True)
        final_df['Date'] = pd.to_datetime(final_df['Date'])
        return final_df

    df = load_trend_data()

    if df.empty:
        st.warning("데이터가 없거나 불러오지 못했습니다.")
        return

    # --- Filters ---
    st.subheader("설정 및 필터")
    all_chefs = sorted(df['Chef'].unique())
    selected_chefs = st.multiselect("쉐프 선택 (전체 보기는 비워두세요)", options=all_chefs, default=[])
    
    if selected_chefs:
        plot_df = df[df['Chef'].isin(selected_chefs)]
    else:
        plot_df = df
        
    # --- Visualization ---
    if not plot_df.empty:
        col_wrap = 4
        g = sns.relplot(
            data=plot_df, x="Date", y="Value", hue="Source", col="Chef",
            kind="line", palette={'Google': 'blue', 'Naver': 'green'},
            col_wrap=col_wrap, height=4, aspect=1.5,
            facet_kws={'sharey': False, 'sharex': True}
        )
        g.fig.subplots_adjust(top=0.9)
        for axes in g.axes.flat:
            _ = axes.tick_params(axis='x', rotation=45)
        st.pyplot(g.fig)
    else:
        st.info("선택된 데이터가 없습니다.")

    with st.expander("📊 원본 데이터 테이블"):
        st.dataframe(plot_df)


# --- Main App Structure ---
def main():
    st.sidebar.title("🍳 흑백요리사 분석 (Ver.2)")
    st.sidebar.markdown("---")
    
    # Navigation
    menu = st.sidebar.radio(
        "분석 메뉴 선택",
        ["1. 감자 서바이벌 예측", "2. 쉐프 검색 트렌드"]
    )
    
    st.sidebar.markdown("---")
    st.sidebar.info("새로운 데이터(셰프서바이벌결과요약)가 적용되었습니다.")

    # Routing
    if menu == "1. 감자 서바이벌 예측":
        show_survival_analysis()
    elif menu == "2. 쉐프 검색 트렌드":
        show_trend_analysis()

if __name__ == "__main__":
    main()
