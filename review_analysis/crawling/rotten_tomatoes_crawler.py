from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
import time
import re
import datetime
import os
import pandas as pd # type: ignore
from review_analysis.crawling.base_crawler import BaseCrawler
from utils.logger import setup_logger

from typing import List, Dict, Any

class RottenTomatoesCrawler(BaseCrawler):
    """
    로튼 토마토(Rotten Tomatoes) 사이트에서 영화 '주토피아(Zootopia)'의 관람객 리뷰를 수집하여 CSV 파일로 저장하는 클래스.
    평점, 작성일, 리뷰 내용을 포함합니다.

    Attributes:
        - driver (webdriver.Chrome): 크롬 웹드라이버 인스턴스.
        - url (str): 크롤링 대상 URL.
        - data (list): 수집된 리뷰 데이터를 저장하는 리스트.
    
    Methods:
        - start_browser: 웹드라이버를 설정하고 실행.
        - parse_date: 날짜 문자열을 파싱하여 정형화된 포맷으로 변환.
        - scrape_reviews: 실제 크롤링 로직을 수행 (리뷰 수집 및 페이징).
        - save_to_database: 수집된 데이터를 CSV 파일로 저장.
    """
    def __init__(self, output_dir: str):
        super().__init__(output_dir)
        self.logger = setup_logger()
        self.url = "https://www.rottentomatoes.com/m/zootopia/reviews/all-audience"
        self.driver = None
        self.data: List[Dict[str, Any]] = []

    def start_browser(self):
        """
        크롬 웹드라이버를 설정하고 실행하는 메소드.
        """
        chrome_options = webdriver.ChromeOptions()
        chrome_options.add_experimental_option("detach", True)
        chrome_options.add_experimental_option("excludeSwitches", ["enable-logging"])
        
        self.driver = webdriver.Chrome(
            service=Service(ChromeDriverManager().install()),
            options=chrome_options
        )
        self.driver.maximize_window()

    def parse_date(self, date_str, current_year):
        """
        다양한 형식의 날짜 문자열(상대 시간, 부분 날짜, 전체 날짜)을 'YYYY.MM.DD.' 형식으로 파싱하는 메소드.

        Args:
            date_str (str): 원본 날짜 문자열 (예: "Jan 8", "8h ago", "12/12/2024").
            current_year (int): 현재 처리 중인 연도 (부분 날짜 파싱 시 사용).

        Returns:
            tuple: (파싱된 날짜 문자열, 업데이트된 연도) 튜플 반환.
        """
        today = datetime.datetime.now()
        
        try:
            date_str = date_str.strip()
            
            # 케이스 1: 상대 시간 (시간 단위, 예: "8h ago")
            if 'h' in date_str:
                return today.strftime("%Y.%m.%d."), current_year
                
            # 케이스 2: 상대 시간 (일 단위, 예: "6d ago")
            if 'd' in date_str:
                days_ago_match = re.search(r'(\d+)d', date_str)
                if days_ago_match:
                    days_ago = int(days_ago_match.group(1))
                    target_date = today - datetime.timedelta(days=days_ago)
                    return target_date.strftime("%Y.%m.%d."), current_year
            
            # 케이스 3: 전체 날짜 (예: "12/12/2024")
            if re.match(r'\d{1,2}/\d{1,2}/\d{4}', date_str):
                parsed_date = datetime.datetime.strptime(date_str, "%m/%d/%Y")
                return parsed_date.strftime("%Y.%m.%d."), parsed_date.year

            # 케이스 4: 부분 날짜 (예: "Jan 8")
            # 배지 텍스트 제거
            date_str = re.sub(r'Verified|Super Reviewer', '', date_str).strip()
            
            try:
                # "Jan 8" 형식 파싱
                parsed_date = datetime.datetime.strptime(date_str, "%b %d")
                final_date = parsed_date.replace(year=current_year)
                return final_date.strftime("%Y.%m.%d."), current_year
                
            except ValueError:
                return f"{current_year}.{date_str}", current_year # 폴백
                
        except Exception as e:
            self.logger.error(f"날짜 파싱 오류 '{date_str}': {e}")
            return date_str, current_year

    def scrape_reviews(self):
        """
        로튼 토마토 사이트에서 리뷰를 크롤링하는 메인 로직을 수행하는 메소드. 
        'Load More' 버튼을 클릭하며 모든 리뷰를 수집함.
        """
        self.start_browser()
        self.logger.info(f"{self.url}로 이동 중")
        self.driver.get(self.url)
        time.sleep(3) # 초기 로딩 대기

        # 쿠키 팝업 처리
        try:
            cookie_btn = self.driver.find_element(By.ID, "onetrust-accept-btn-handler")
            cookie_btn.click()
            self.logger.info("쿠키 승인 버튼 클릭됨")
            time.sleep(1)
        except:
            self.logger.info("쿠키 팝업이 없거나 이미 처리됨")

        processed_indices = set()
        current_year = 2026
        last_month_num = 1
        
        while True:
            try:
                # 모든 리뷰 카드 찾기
                reviews = self.driver.find_elements(By.CSS_SELECTOR, "review-card")
                new_reviews_found = False
                
                for i, review in enumerate(reviews):
                    if i in processed_indices:
                        continue
                    
                    try:
                        # 데이터 추출
                        # 평점 추출 (여러 선택자 시도)
                        try:
                            rating_element = review.find_element(By.CSS_SELECTOR, "[slot='rating']")
                            rating = rating_element.get_attribute("score") or rating_element.get_attribute("rating") or ""
                        except:
                            rating = "" # 일부 리뷰는 평점이 없을 수 있음
                        
                        try:
                            date_element = review.find_element(By.CSS_SELECTOR, "[slot='timestamp']")
                            raw_date = date_element.text
                            
                            # 부분 날짜에서 연도 변경 감지 (예: 1월 -> 12월로 넘어갈 때)
                            if "/" not in raw_date:
                                try:
                                    current_month = 1 # 기본값
                                    if "Jan" in raw_date: current_month = 1
                                    elif "Feb" in raw_date: current_month = 2
                                    elif "Mar" in raw_date: current_month = 3
                                    elif "Apr" in raw_date: current_month = 4
                                    elif "May" in raw_date: current_month = 5
                                    elif "Jun" in raw_date: current_month = 6
                                    elif "Jul" in raw_date: current_month = 7
                                    elif "Aug" in raw_date: current_month = 8
                                    elif "Sep" in raw_date: current_month = 9
                                    elif "Oct" in raw_date: current_month = 10
                                    elif "Nov" in raw_date: current_month = 11
                                    elif "Dec" in raw_date: current_month = 12
                                    else: current_month = last_month_num 

                                    # 월이 급격히 증가하면 연도가 바뀐 것으로 간주
                                    if current_month > last_month_num + 6:
                                         current_year -= 1
                                         self.logger.info(f"연도가 {current_year}년으로 변경되었습니다.")
                                    
                                    last_month_num = current_month
                                except:
                                    pass

                            date, current_year = self.parse_date(raw_date, current_year)
                            
                        except Exception as e:
                            self.logger.error(f"날짜 처리 오류: {e}")
                            date = ""

                        try:
                            comment_element = review.find_element(By.CSS_SELECTOR, "[slot='content']")
                            comment = comment_element.text
                        except:
                            comment = ""
                        
                        if comment or rating: # 유효한 내용이 있는 경우만 저장
                            self.logger.info(f"리뷰 {i+1}: {rating} | {date}")
                            self.data.append({
                                'rating': rating,
                                'date': date,
                                'comment': comment
                            })
                            new_reviews_found = True
                        
                        processed_indices.add(i)

                    except Exception as e:
                        self.logger.error(f"{i}번 리뷰 파싱 중 오류 발생: {e}")
                
                # 중간 저장
                if new_reviews_found and len(self.data) % 50 == 0:
                    self.save_to_database()

                # 'Load More' 버튼 찾기 및 클릭
                try:
                    load_more_btn = None
                    try:
                         load_more_btn = self.driver.find_element(By.CSS_SELECTOR, "rt-button[data-pagemediareviewsmanager='loadMoreBtn:click']")
                    except:
                        # 폴백: 텍스트 내용으로 확인
                        buttons = self.driver.find_elements(By.TAG_NAME, "rt-button")
                        for btn in buttons:
                            if "Load More" in btn.text:
                                load_more_btn = btn
                                break
                    
                    if load_more_btn:
                        # 버튼 위치로 스크롤
                        self.driver.execute_script("arguments[0].scrollIntoView(true);", load_more_btn)
                        time.sleep(1)
                        # 클릭
                        self.driver.execute_script("arguments[0].click();", load_more_btn)
                        self.logger.info("'Load More' 클릭됨")
                        time.sleep(3) # 새 리뷰 로딩 대기
                    else:
                        self.logger.info("'Load More' 버튼을 찾을 수 없습니다.")
                        break
                except Exception as e:
                    self.logger.info(f"'Load More' 처리 중 오류: {e}")
                    break
                    
            except Exception as e:
                self.logger.error(f"크롤링 루프 중 치명적 오류 발생: {e}")
                break

    def save_to_database(self):
        """
        수집된 리뷰 데이터를 CSV 파일로 저장하는 메소드.
        """
        if not self.data:
            self.logger.warning("저장할 데이터가 없습니다.")
            return

        if not os.path.exists(self.output_dir):
            os.makedirs(self.output_dir)
            
        filename = "reviews_RottenTomatoes.csv"
        filepath = os.path.join(self.output_dir, filename)
        
        df = pd.DataFrame(self.data)
        df.to_csv(filepath, index=False, encoding='utf-8-sig')
        self.logger.info(f"{len(self.data)}개의 리뷰를 {filepath}에 저장 완료")
