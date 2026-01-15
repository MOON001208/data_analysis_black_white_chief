import pandas as pd
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import time
import datetime
import os
import re

# ==========================================
# 설정
# ==========================================
INPUT_FILE = "캐치테이블_가게정보.csv"
HISTORY_FILE = "review_count_history.csv"
OUTPUT_FILE = f"reviews_collected_{datetime.datetime.now().strftime('%Y%m%d')}.csv"
CUTOFF_DATE = datetime.datetime(2025, 12, 9)

# ==========================================
# 함수 정의
# ==========================================

def get_driver():
    options = Options()
    options.add_argument("--headless") # Airflow 등 서버 환경에서는 주석 해제 필요
    options.add_argument("--window-size=1600,1000")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    driver = webdriver.Chrome(options=options)
    return driver

def load_history():
    if os.path.exists(HISTORY_FILE):
        return pd.read_csv(HISTORY_FILE)
    else:
        return pd.DataFrame(columns=['url', 'restaurant_name', 'review_count', 'last_updated'])

def save_history(df):
    df.to_csv(HISTORY_FILE, index=False, encoding='utf-8-sig')

def modify_url_for_reviews(url):
    """
    가게 상세 URL을 리뷰 모아보기 URL로 변환
    규칙: '?type' 앞에 'review' 추가, 끝에 '&sortingFilter=D' (최신순) 추가
    """
    if "/review" in url:
        if "sortingFilter=D" not in url:
             return url + "&sortingFilter=D"
        return url
    if "?" in url:
        base, query = url.split("?", 1)
        if base.endswith("/"): base = base[:-1]
        new_url = f"{base}/review?{query}&sortingFilter=D"
    else:
        if url.endswith("/"): url = url[:-1]
        new_url = f"{url}/review?sortingFilter=D"
    return new_url

def parse_date(date_str):
    today = datetime.datetime.now()
    try:
        if "일 전" in date_str:
            days = int(re.search(r'(\d+)', date_str).group(1))
            return today - datetime.timedelta(days=days)
        elif "시간 전" in date_str or "분 전" in date_str or "방금" in date_str:
            return today
        elif "어제" in date_str:
            return today - datetime.timedelta(days=1)
        else:
            return datetime.datetime.strptime(date_str, "%Y.%m.%d")
    except:
        return today

def scrape_reviews(driver, url, restaurant_name):
    """리뷰 상세 페이지 크롤링 함수 (Notebook Logic Ported)"""
    review_url = modify_url_for_reviews(url)
    driver.get(review_url)
    time.sleep(5)
    
    collected_reviews = []
    processed_hashes = set()
    
    scrolling = True
    scroll_count = 0
    max_scrolls = 50 
    
    while scrolling and scroll_count < max_scrolls:
        try:
            # 검증된 카드 셀렉터 (Notebook과 동일)
            cards = driver.find_elements(By.CSS_SELECTOR, "#main > div.container.gutter-sm > div > div > div > div")
        except:
            cards = []
            
        # 첫 시도 시 카드 없으면 로딩 대기
        if not cards and scroll_count == 0:
            time.sleep(3)
            try:
                cards = driver.find_elements(By.CSS_SELECTOR, "#main > div.container.gutter-sm > div > div > div > div")
            except:
                pass
        
        for card in cards:
            try:
                # 1. 작성자
                try:
                    reviewer = card.find_element(By.CSS_SELECTOR, "article > div.__header > div.__user-info > a > h4 > span").text
                except:
                    reviewer = "Unknown"

                # 2. 평점
                try:
                    rating_el = card.find_element(By.CSS_SELECTOR, "article > div.__header > div.__review-meta.__review-meta--with-rating > div > a > div")
                    rating = rating_el.text
                except:
                    rating = "Unknown"

                # 3. 날짜
                try:
                    date_el = card.find_element(By.CSS_SELECTOR, "article > div.__header > div.__review-meta.__review-meta--with-rating > span")
                    date_text = date_el.text
                except:
                    match = re.search(r'\d{4}\.\d{1,2}\.\d{1,2}|\d+일 전', card.text)
                    date_text = match.group(0) if match else "Unknown"

                if date_text != "Unknown":
                    review_date = parse_date(date_text)
                    if review_date < CUTOFF_DATE:
                        scrolling = False 
                        break

                # 4. 방문 유형(점심/저녁)
                try:
                    day_night = card.find_element(By.CSS_SELECTOR, "article > div.__header > div.__review-meta.__review-meta--with-rating > div > p").text
                except:
                    day_night = "Unknown"

                # 중복 수집 방지
                msg_hash = hash(f"{reviewer}_{date_text}_{restaurant_name}")
                if msg_hash not in processed_hashes:
                    processed_hashes.add(msg_hash)
                    collected_reviews.append({
                        "restaurant": restaurant_name,
                        "reviewer": reviewer,
                        "review_date": date_text,
                        "reviewer_rating": rating,
                        "day_night": day_night
                    })
            except:
                continue
        
        if not scrolling:
            break
            
        # 스크롤 동작 (ActionChains)
        try:
            if cards:
                last_card = cards[-1]
                actions = ActionChains(driver)
                actions.move_to_element(last_card).perform()
                time.sleep(0.5)
                actions.send_keys(Keys.PAGE_DOWN).pause(0.5).send_keys(Keys.PAGE_DOWN).perform()
                time.sleep(1.5)
            else:
                driver.find_element(By.TAG_NAME, "body").send_keys(Keys.PAGE_DOWN)
                time.sleep(1)
        except:
             driver.find_element(By.TAG_NAME, "body").send_keys(Keys.PAGE_DOWN)
        
        scroll_count += 1
        
    return collected_reviews

def main():
    if not os.path.exists(INPUT_FILE):
        print(f"오류: {INPUT_FILE} 파일이 없습니다.")
        return

    df = pd.read_csv(INPUT_FILE)
    history_df = load_history()
    
    driver = get_driver()
    
    all_collected_data = []
    history_updates = []
    
    print(f"총 {len(df)}개 식당 처리 시작...")
    
    try:
        for idx, row in df.iterrows():
            url = row['URL']
            restaurant_name = row['restaurant'] if 'restaurant' in row else str(idx)
            
            try:
                # 1. 메인 접속
                driver.get(url)
                time.sleep(3)
                
                # 2. 리뷰 개수 확인 (Notebook의 검증된 XPath 사용)
                try:
                    count_el = driver.find_element(By.XPATH, '//*[@id="wrapperDiv"]/div[1]/div[1]/div[3]/div/span[3]')
                    count_text = count_el.text
                    review_count = int(re.search(r'(\d+)', count_text.replace(',', '')).group(1))
                except:
                    review_count = 0
                
                print(f"[{idx+1}/{len(df)}] {restaurant_name}: {review_count}개", end=" ")
                
                # 3. 변경 감지
                prev_record = history_df[history_df['url'] == url]
                need_crawl = False
                
                if prev_record.empty:
                    print("-> [신규]", end=" ")
                    need_crawl = True
                else:
                    last_count = int(prev_record.iloc[0]['review_count'])
                    if last_count != review_count:
                        print(f"-> [변동: {last_count}->{review_count}]", end=" ")
                        if review_count > 0: need_crawl = True
                    else:
                        print("-> [변동없음]", end=" ")
                
                history_updates.append({
                    'url': url,
                    'restaurant_name': restaurant_name,
                    'review_count': review_count,
                    'last_updated': datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                })
                
                # 4. 크롤링 수행
                if need_crawl:
                    print("-> 수집 시작")
                    reviews = scrape_reviews(driver, url, restaurant_name)
                    if reviews:
                        all_collected_data.extend(reviews)
                        print(f"   >>> {len(reviews)}건 수집 완료")
                    else:
                        print("   >>> 수집된 리뷰 없음")
                else:
                    print("") 
                    
            except Exception as e:
                print(f"\n   !!! 에러 발생: {e}")

    finally:
        driver.quit()
        
    # 결과 저장
    if all_collected_data:
        result_df = pd.DataFrame(all_collected_data)
        result_df.to_csv(OUTPUT_FILE, index=False, encoding='utf-8-sig')
        print(f"\n전체 결과 저장 완료: {OUTPUT_FILE} ({len(result_df)}건)")
    else:
        print("\n새로 수집된 리뷰가 없습니다.")
        
    # 히스토리 업데이트
    if history_updates:
        new_history = pd.DataFrame(history_updates)
        if not history_df.empty:
            history_df = history_df[~history_df['url'].isin(new_history['url'])]
            final_history = pd.concat([history_df, new_history], ignore_index=True)
        else:
            final_history = new_history
        save_history(final_history)
        print("히스토리 업데이트 완료")

if __name__ == "__main__":
    main()
