"""
흑백요리사2 - 서울시 유동인구 자동 수집 및 Supabase 적재 DAG
"""
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.hooks.base import BaseHook
import requests
import pandas as pd
import os
import time
import json

# ==============================================================================
# Configuration
# ==============================================================================
SERVICE = "IotVdata018"
BASE_URL = "http://openapi.seoul.go.kr:8088/{key}/json/{service}/{start}/{end}/"
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
OUTPUT_FILE = os.path.join(BASE_DIR, "seoul_floating_pop_raw3.csv")
SEOUL_API_CONN_ID = "seoul_api"  # Seoul API Key Connection
SUPABASE_CONN_ID = "xoosl033110_supabase_conn"
SUPABASE_TABLE = "seoul_floating_population"

# 자치구 영-한 매핑
GU_MAPPING = {
    'gangnam-gu': '강남구', 'gangdong-gu': '강동구', 'gangbuk-gu': '강북구',
    'gangseo-gu': '강서구', 'gwanak-gu': '관악구', 'gwangjin-gu': '광진구',
    'guro-gu': '구로구', 'geumcheon-gu': '금천구', 'nowon-gu': '노원구',
    'dobong-gu': '도봉구', 'dongdaemun-gu': '동대문구', 'dongjak-gu': '동작구',
    'mapo-gu': '마포구', 'seodaemun-gu': '서대문구', 'seocho-gu': '서초구',
    'seongdong-gu': '성동구', 'seongbuk-gu': '성북구', 'songpa-gu': '송파구',
    'yangcheon-gu': '양천구', 'yeongdeungpo-gu': '영등포구', 'yongsan-gu': '용산구',
    'eunpyeong-gu': '은평구', 'jongno-gu': '종로구', 'jung-gu': '중구', 'jungnang-gu': '중랑구'
}


def get_seoul_api_key():
    """Airflow Connection에서 Seoul API Key 가져오기"""
    conn = BaseHook.get_connection(SEOUL_API_CONN_ID)
    # password 또는 extra에서 API Key 추출
    return conn.password or conn.extra


def get_supabase_credentials():
    """Airflow Connection에서 Supabase 인증정보 가져오기"""
    conn = BaseHook.get_connection(SUPABASE_CONN_ID)
    supabase_url = conn.host if conn.host.startswith('http') else f"https://{conn.host}"
    supabase_key = conn.password
    return supabase_url, supabase_key


def translate_name(name, mapping_dict):
    """영문 지명을 한글로 변환"""
    if not name:
        return ""
    norm_name = name.lower().replace(' ', '')
    return mapping_dict.get(norm_name, name)


def parse_sensing_time(time_str):
    """시간 문자열 파싱"""
    if not time_str:
        return None
    for fmt in ["%Y-%m-%d_%H:%M:%S", "%Y-%m-%d %H:%M:%S"]:
        try:
            return datetime.strptime(time_str, fmt)
        except ValueError:
            continue
    return None


def fetch_data_batch(api_key, start_idx, end_idx, retries=3):
    """API에서 데이터 배치 가져오기"""
    url = BASE_URL.format(key=api_key, service=SERVICE, start=start_idx, end=end_idx)
    
    for attempt in range(retries):
        try:
            response = requests.get(url, timeout=30)
            response.raise_for_status()
            data = response.json()
            
            if SERVICE in data and 'row' in data[SERVICE]:
                return data[SERVICE]['row']
            elif 'RESULT' in data and data['RESULT'].get('CODE') == 'INFO-200':
                return []
            else:
                time.sleep(1)
                continue
        except Exception as e:
            if attempt == retries - 1:
                print(f"[Error] Failed to fetch {start_idx}-{end_idx}: {e}")
            time.sleep(2)
    return []


def get_latest_collected_time():
    """기존 파일에서 최신 수집 시간 조회"""
    if not os.path.exists(OUTPUT_FILE):
        return datetime(2025, 12, 9)
    
    try:
        df = pd.read_csv(OUTPUT_FILE, usecols=['SENSING_TIME'])
        if df.empty:
            return datetime(2025, 12, 9)
        
        df['SENSING_TIME'] = pd.to_datetime(df['SENSING_TIME'], format="%Y-%m-%d %H:%M:%S", errors='coerce')
        max_time = df['SENSING_TIME'].max()
        
        return max_time if pd.notna(max_time) else datetime(2025, 12, 9)
    except Exception as e:
        print(f"[Warning] Could not read existing file: {e}")
        return datetime(2025, 12, 9)


def collect_population_data(**context):
    """유동인구 데이터 수집"""
    print("=== Starting Seoul IoT Population Data Collection ===")
    
    # Seoul API Key 가져오기
    api_key = get_seoul_api_key()
    print(f"[Info] Seoul API Key loaded from Airflow Connection")
    
    last_collected_time = get_latest_collected_time()
    print(f"Latest collected time: {last_collected_time}")
    
    if not os.path.exists(OUTPUT_FILE):
        header = ["SENSING_TIME", "AUTONOMOUS_DISTRICT", "ADMINISTRATIVE_DISTRICT", "VISITOR_COUNT", "REG_DTTM"]
        pd.DataFrame(columns=header).to_csv(OUTPUT_FILE, index=False, encoding='utf-8-sig')
    
    start_idx = 1
    batch_size = 1000
    total_new_rows = 0
    all_new_data = []
    max_batches = 100
    
    for batch_num in range(max_batches):
        end_idx = start_idx + batch_size - 1
        rows = fetch_data_batch(api_key, start_idx, end_idx)  # api_key 전달
        
        if not rows:
            print(f"[Info] No more data at index {start_idx}")
            break
        
        valid_rows = []
        stop_signal = False
        
        for row in rows:
            sensing_raw = row.get('SENSING_TIME') or row.get('REG_DTTM')
            dt = parse_sensing_time(sensing_raw)
            
            if not dt:
                continue
            
            if dt <= last_collected_time:
                stop_signal = True
                break
            
            raw_gu = row.get('AUTONOMOUS_DISTRICT', '')
            kor_gu = translate_name(raw_gu, GU_MAPPING)
            
            valid_rows.append({
                "SENSING_TIME": dt.strftime("%Y-%m-%d %H:%M:%S"),
                "AUTONOMOUS_DISTRICT": kor_gu,
                "ADMINISTRATIVE_DISTRICT": row.get('ADMINISTRATIVE_DISTRICT', ''),
                "VISITOR_COUNT": row.get('VISITOR_COUNT', 0),
                "REG_DTTM": row.get('REG_DTTM', '')
            })
        
        if valid_rows:
            df_batch = pd.DataFrame(valid_rows)
            df_batch.to_csv(OUTPUT_FILE, mode='a', header=False, index=False, encoding='utf-8-sig')
            all_new_data.extend(valid_rows)
            total_new_rows += len(valid_rows)
            print(f"[Batch {batch_num + 1}] Added {len(valid_rows)} rows")
        
        if stop_signal:
            print(f"[Info] Reached already collected data. Stopping.")
            break
        
        start_idx += batch_size
        time.sleep(0.2)
    
    # XCom으로 새 데이터 전달 (Supabase 적재용)
    context['ti'].xcom_push(key='new_data', value=all_new_data)
    context['ti'].xcom_push(key='total_rows', value=total_new_rows)
    
    print(f"=== Collection Complete. New rows: {total_new_rows} ===")
    return total_new_rows


def load_to_supabase(**context):
    """수집된 데이터를 Supabase에 적재"""
    print("=== Loading Data to Supabase ===")
    
    # XCom에서 데이터 가져오기
    ti = context['ti']
    new_data = ti.xcom_pull(task_ids='collect_population_data', key='new_data')
    total_rows = ti.xcom_pull(task_ids='collect_population_data', key='total_rows')
    
    if not new_data or total_rows == 0:
        print("[Info] No new data to load to Supabase")
        return 0
    
    # Supabase 인증정보 가져오기
    supabase_url, supabase_key = get_supabase_credentials()
    
    # Supabase REST API 엔드포인트
    api_url = f"{supabase_url}/rest/v1/{SUPABASE_TABLE}"
    headers = {
        "apikey": supabase_key,
        "Authorization": f"Bearer {supabase_key}",
        "Content-Type": "application/json",
        "Prefer": "return=minimal"
    }
    
    # 배치 단위로 삽입 (500개씩)
    batch_size = 500
    inserted_count = 0
    
    for i in range(0, len(new_data), batch_size):
        batch = new_data[i:i + batch_size]
        
        # 컬럼명을 Supabase 테이블에 맞게 변환
        supabase_batch = []
        for row in batch:
            supabase_batch.append({
                "sensing_time": row["SENSING_TIME"],
                "autonomous_district": row["AUTONOMOUS_DISTRICT"],
                "administrative_district": row["ADMINISTRATIVE_DISTRICT"],
                "visitor_count": row["VISITOR_COUNT"],
                "reg_dttm": row["REG_DTTM"],
                "created_at": datetime.now().isoformat()
            })
        
        try:
            response = requests.post(
                api_url,
                headers=headers,
                data=json.dumps(supabase_batch),
                timeout=30
            )
            
            if response.status_code in [200, 201]:
                inserted_count += len(batch)
                print(f"[Supabase] Inserted batch {i // batch_size + 1}: {len(batch)} rows")
            else:
                print(f"[Error] Supabase insert failed: {response.status_code} - {response.text}")
                
        except Exception as e:
            print(f"[Error] Supabase request failed: {e}")
    
    print(f"=== Supabase Load Complete. Inserted: {inserted_count} rows ===")
    return inserted_count


# ==============================================================================
# DAG Definition
# ==============================================================================
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='seoul_population_collector',
    default_args=default_args,
    description='서울시 유동인구 IoT 데이터 수집 및 Supabase 적재',
    schedule_interval='0 */6 * * *',  # 6시간마다 실행
    start_date=datetime(2026, 1, 15),
    catchup=False,
    tags=['흑백요리사', '유동인구', 'IoT', 'Supabase'],
) as dag:
    
    start = EmptyOperator(task_id='start')
    
    collect_data = PythonOperator(
        task_id='collect_population_data',
        python_callable=collect_population_data,
        provide_context=True,
    )
    
    load_supabase = PythonOperator(
        task_id='load_to_supabase',
        python_callable=load_to_supabase,
        provide_context=True,
    )
    
    end = EmptyOperator(task_id='end')
    
    start >> collect_data >> load_supabase >> end
