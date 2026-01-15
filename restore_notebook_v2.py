
import json
import re

# 1. Load Original
input_path = 'prediction_model.ipynb'
output_path = 'predict_model2.ipynb'

with open(input_path, 'r', encoding='utf-8') as f:
    nb = json.load(f)

# 2. Define the plotting function to inject
plot_func_code = """
# 잔차 분석 시각화 함수 (수정됨: 빨간 실선 LOWESS, 파란 점선 기준선)
def plot_residual_plots_like_r(model, X, title_prefix):
    try:
        import seaborn as sns
        import matplotlib.pyplot as plt
        import numpy as np

        residuals = model.resid_pearson
        fitted = model.predict()
        features = [col for col in X.columns if col != 'const']
        n_features = len(features)
        
        # 1. Residuals vs Predictors
        n_cols = 2
        n_rows = (n_features + n_cols - 1) // n_cols
        
        fig, axes = plt.subplots(n_rows, n_cols, figsize=(15, 5 * n_rows))
        fig.suptitle(f'{title_prefix} - Residuals vs Predictors (with LOWESS)', fontsize=16)
        
        axes = axes.flatten()
        
        for i, feature in enumerate(features):
            ax = axes[i]
            sns.regplot(
                x=X[feature], 
                y=residuals, 
                lowess=True, 
                ax=ax,
                scatter_kws={'alpha': 0.3},
                line_kws={'color': 'red', 'linewidth': 2}
            )
            ax.axhline(0, color='blue', linestyle='--', linewidth=1)
            ax.set_title(f'Residuals vs {feature}')
            ax.set_xlabel(feature)
            ax.set_ylabel('Pearson Residuals')
            
        for j in range(i + 1, len(axes)):
            axes[j].set_visible(False)
            
        plt.tight_layout()
        plt.subplots_adjust(top=0.95)
        plt.show()

        # 2. Residuals vs Fitted
        plt.figure(figsize=(10, 6))
        sns.regplot(
            x=fitted.values if hasattr(fitted, 'values') else np.array(fitted), 
            y=residuals.values if hasattr(residuals, 'values') else np.array(residuals), 
            lowess=True, 
            scatter_kws={'alpha': 0.5},
            line_kws={'color': 'red', 'linewidth': 2}
        )
        plt.axhline(0, color='blue', linestyle='--', linewidth=1)
        plt.title(f'{title_prefix} - Residuals vs Fitted')
        plt.xlabel('Fitted Values (Predicted Probability)')
        plt.ylabel('Pearson Residuals')
        plt.show()
        
    except Exception as e:
        print(f"잔차 분석 중 오류 발생: {e}")
"""

# 3. Create new cell for function
new_cell = {
    "cell_type": "code",
    "execution_count": None,
    "metadata": {},
    "outputs": [],
    "source": plot_func_code.splitlines(keepends=True)
}

# 4. Modify Cells
cells = nb['cells']
# Insert the function code AFTER the first code cell (assuming imports are in cell 0 or 1)
# Find first code cell
insert_idx = 0
for i, cell in enumerate(cells):
    if cell['cell_type'] == 'code':
        insert_idx = i + 1
        break
cells.insert(insert_idx, new_cell)

for cell in cells:
    # (A) Change file path
    if cell['cell_type'] == 'code':
        source = "".join(cell['source'])
        if '3번문제완성본.csv' in source:
            cell['source'] = source.replace('3번문제완성본.csv', '셰프서바이벌결과요약.csv').splitlines(keepends=True)
            
    # (B) Update Conclusion
    if cell['cell_type'] == 'markdown':
        source = "".join(cell['source'])
        if "결론" in source or "Conclusion" in source:
            new_conclusion = """
# 📊 최종 분석 결론 (Conclusion) - `셰프서바이벌결과요약.csv` 분석

데이터 분석 결과, 두 심사위원의 합격 기준은 다음과 같이 요약됩니다.

### 1. 안성재 심사위원 공략
*   **핵심**: **조림(Braising)** 방식을 선호하며, 튀김 요리를 극도로 기피합니다.
*   **전략**: **"기본에 충실하라."** 정성이 들어간 한식/퓨전 베이스의 **조림 요리**가 가장 안전한 합격 티켓입니다. 튀김은 피하십시오.

### 2. 백종원 심사위원 공략
*   **핵심**: **퓨전(Fusion)**과 **튀김(Frying)**을 선호합니다.
*   **전략**: **"창의적이고 직관적인 맛."** 기존에 없던 조합의 **퓨전 요리**나, 강력한 불맛/튀김 기술이 들어간 **중식 요리**로 승부하십시오.

> **요약**: 안성재에게는 '깊은 맛(조림)', 백종원에게는 '새로운 맛(퓨전/튀김)'을 보여주는 것이 필승 전략입니다.
"""
            cell['source'] = new_conclusion.splitlines(keepends=True)

# 5. Save
with open(output_path, 'w', encoding='utf-8') as f:
    json.dump(nb, f, ensure_ascii=False, indent=1)

print("Restored predict_model2.ipynb with fixes.")
