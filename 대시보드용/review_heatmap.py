"""
흑백요리사2 대시보드 - 리뷰 히트맵 시각화 모듈
"""
import pandas as pd
import plotly.graph_objects as go
import plotly.express as px
from typing import Optional
import sys
import os

# 모듈 경로 추가
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
from data_processor import (
    load_reviews, 
    load_restaurants, 
    calculate_review_changes,
    BROADCAST_DATES
)


def create_review_heatmap(
    df_changes: pd.DataFrame,
    df_restaurants: Optional[pd.DataFrame] = None,
    value_column: str = 'change_rate',
    title: str = '흑백요리사2 방영일별 리뷰 변화 히트맵',
    min_reviews: int = 3,  # 최소 리뷰 수 필터
    clip_range: tuple = (-100, 150),  # 증가율 클리핑 범위
    max_restaurants: Optional[int] = None,
) -> go.Figure:
    """리뷰 변화 히트맵 생성"""
    # 최소 리뷰 수 필터링
    df_filtered = df_changes.copy()
    df_filtered['total_reviews'] = df_filtered['before_count'] + df_filtered['after_count']
    
    valid_restaurants = df_filtered.groupby('restaurant')['total_reviews'].sum()
    valid_restaurants = valid_restaurants[valid_restaurants >= min_reviews].sort_values(ascending=False)
    if max_restaurants is not None:
        valid_restaurants = valid_restaurants.head(max_restaurants)
    valid_restaurants = valid_restaurants.index.tolist()
    df_filtered = df_filtered[df_filtered['restaurant'].isin(valid_restaurants)]
    
    if len(df_filtered) == 0:
        fig = go.Figure()
        fig.add_annotation(text="표시할 데이터가 없습니다", x=0.5, y=0.5, showarrow=False)
        return fig
    
    # 극단값 클리핑
    if value_column == 'change_rate':
        df_filtered['value'] = df_filtered['change_rate'].clip(clip_range[0], clip_range[1])
    else:
        df_filtered['value'] = df_filtered[value_column]
    
    pivot = df_filtered.pivot(index='restaurant', columns='episode', values='value')
    display_pivot = df_filtered.pivot(index='restaurant', columns='episode', values=value_column)
    
    # 셰프 정보 매핑
    if df_restaurants is not None:
        chef_map = df_restaurants.set_index('restaurant')['chief_info'].to_dict()
        labels = pivot.index.map(
            lambda x: f"{chef_map.get(x, '')} ({x})" if chef_map.get(x) else x
        )
        pivot.index = labels
        display_pivot.index = labels
    
    # 컬럼명 변경
    episode_labels = [f"{i}회 ({bd[5:]})" for i, bd in enumerate(BROADCAST_DATES, 1)]
    pivot.columns = episode_labels
    display_pivot.columns = episode_labels
    
    # 히트맵 생성 - RdBu 색상으로 명확하게
    # 표시 형식 결정 (증가율이면 %, 증가 수면 개수)
    if value_column == 'change_rate':
        text_display = [[f"{v:.0f}%" if pd.notna(v) else "" for v in row] for row in display_pivot.values]
        hover_template = '<b>%{y}</b><br>%{x}: %{customdata:.1f}%<extra></extra>'
        colorbar_title = '증가율 (%)'
        colorbar_tickvals = [-100, -50, 0, 50, 100, 150]
        colorbar_ticktext = ['-100%', '-50%', '0%', '+50%', '+100%', '+150%']
    else:
        text_display = [[f"{v:.0f}" if pd.notna(v) else "" for v in row] for row in display_pivot.values]
        hover_template = '<b>%{y}</b><br>%{x}: %{z:.0f}개<extra></extra>'
        colorbar_title = '증가 수 (개)'
        max_count = int(pivot.values.max())
        colorbar_tickvals = [0, max_count//2, max_count]
        colorbar_ticktext = ['0', f'{max_count//2}', f'{max_count}']

    fig = go.Figure(data=go.Heatmap(
        z=pivot.values,
        customdata=display_pivot.values,
        x=pivot.columns,
        y=pivot.index,
        colorscale='RdBu_r',  # 빨강-흰색-파랑 (역순이라 빨강=증가)
        zmid=0,
        zmin=clip_range[0] if value_column == 'change_rate' else 0,
        zmax=clip_range[1] if value_column == 'change_rate' else None,
        text=text_display,
        texttemplate='%{text}',
        textfont={"size": 10, "color": "black"},
        hoverongaps=False,
        hovertemplate=hover_template,
        colorbar=dict(
            title=colorbar_title,
            thickness=15,
            tickvals=colorbar_tickvals,
            ticktext=colorbar_ticktext
        )
    ))
    
    fig.update_layout(
        title=dict(text=f"{title}", font=dict(size=16, color='black'), x=0.5),
        xaxis=dict(title='', tickfont=dict(size=11, color='black'), side='top'),
        yaxis=dict(title='', tickfont=dict(size=9, color='black'), autorange='reversed'),
        height=max(400, len(pivot) * 18),
        margin=dict(l=220, r=60, t=80, b=20),
        paper_bgcolor='white',
        plot_bgcolor='white'
    )
    
    return fig




def create_review_bar_chart(
    df_changes: pd.DataFrame,
    selected_restaurant: str = None
) -> go.Figure:
    """
    특정 가게의 방영일별 리뷰 수 비교 막대 그래프
    
    Args:
        df_changes: calculate_review_changes() 결과
        selected_restaurant: 선택된 가게명
    
    Returns:
        Plotly Figure 객체
    """
    if selected_restaurant:
        df = df_changes[df_changes['restaurant'] == selected_restaurant]
    else:
        # 전체 합계
        df = df_changes.groupby('episode').agg({
            'before_count': 'sum',
            'after_count': 'sum',
            'broadcast_date': 'first'
        }).reset_index()
        selected_restaurant = '전체 가게'
    
    fig = go.Figure()
    
    # 방영 전 막대
    fig.add_trace(go.Bar(
        name='방영 7일 전',
        x=[f"{row['episode']}회" for _, row in df.iterrows()],
        y=df['before_count'],
        marker_color='#3498db',
        text=df['before_count'],
        textposition='outside'
    ))
    
    # 방영 후 막대
    fig.add_trace(go.Bar(
        name='방영 7일 후',
        x=[f"{row['episode']}회" for _, row in df.iterrows()],
        y=df['after_count'],
        marker_color='#e74c3c',
        text=df['after_count'],
        textposition='outside'
    ))
    
    # Y축 최대값에 여유 공간 추가 (텍스트가 잘리지 않도록)
    max_value = max(df['before_count'].max(), df['after_count'].max())
    y_range = [0, max_value * 1.15]  # 15% 여유 공간

    fig.update_layout(
        title=f'{selected_restaurant} - 방영 전후 리뷰 수 비교',
        xaxis_title='방영 회차',
        yaxis_title='리뷰 수',
        yaxis=dict(range=y_range),
        barmode='group',
        legend=dict(
            orientation='h',
            yanchor='bottom',
            y=1.02,
            xanchor='right',
            x=1
        ),
        height=500,
        margin=dict(l=60, r=40, t=80, b=60)
    )
    
    return fig


def get_top_restaurants_by_change(
    df_changes: pd.DataFrame, 
    episode: int = None,
    top_n: int = 10,
    ascending: bool = False
) -> pd.DataFrame:
    """
    리뷰 증가율 상위/하위 가게 조회
    
    Args:
        df_changes: calculate_review_changes() 결과
        episode: 특정 회차 (None이면 전체 평균)
        top_n: 상위/하위 N개
        ascending: True면 하위, False면 상위
    
    Returns:
        상위/하위 가게 DataFrame
    """
    if episode:
        df = df_changes[df_changes['episode'] == episode]
    else:
        df = df_changes.groupby('restaurant').agg({
            'change_rate': 'mean',
            'change_count': 'sum'
        }).reset_index()
    
    return df.nlargest(top_n, 'change_rate') if not ascending else df.nsmallest(top_n, 'change_rate')


if __name__ == '__main__':
    print("리뷰 데이터 로드 중...")
    reviews = load_reviews()
    restaurants = load_restaurants()
    
    print("리뷰 변화 계산 중...")
    changes = calculate_review_changes(reviews)
    
    print(f"총 {len(changes['restaurant'].unique())}개 가게 분석 완료")
    
    # 상위 5개 가게 출력
    top5 = get_top_restaurants_by_change(changes, top_n=5)
    print("\n📈 리뷰 증가율 TOP 5:")
    for _, row in top5.iterrows():
        print(f"  - {row['restaurant']}: 평균 {row['change_rate']:.1f}% 증가")
    
    # 히트맵 생성 테스트
    fig = create_review_heatmap(changes, restaurants)
    fig.write_html('review_heatmap_test.html')
    print("\n✅ 히트맵 저장: review_heatmap_test.html")
