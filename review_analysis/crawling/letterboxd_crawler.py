from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
import time
import random
import os
import pandas as pd # type: ignore
from review_analysis.crawling.base_crawler import BaseCrawler

from typing import List, Dict, Any

class LetterboxdCrawler(BaseCrawler):
    """
    Letterboxd 영화 리뷰 사이트에서 리뷰 데이터를 수집하여 CSV 파일로 저장하는 클래스.
    평점, 작성일, 리뷰 내용을 포함합니다.

    Attributes:
        - base_url (str): 크롤링 대상인 Letterboxd 리뷰 페이지 URL.
        - driver (webdriver.Chrome): Selenium 웹드라이버 인스턴스.
        - data (list): 수집된 리뷰 데이터를 저장하는 리스트.
    
    Methods:
        - start_browser: 브라우저를 실행하고 대상 URL로 이동.
        - scrape_reviews: 페이지를 이동하며 리뷰 데이터를 수집.
        - save_to_database: 수집된 데이터를 CSV 파일로 저장.
    """
    def __init__(self, output_dir: str):
        """
        LetterboxdCrawler 객체 초기화

        Arg:
            output_dir: 수집한 리뷰 데이터를 저장할 디렉토리 경로
        """
        super().__init__(output_dir)
        self.base_url = 'https://letterboxd.com/film/zootopia/reviews/'
        self.driver = None
        self.data: List[Dict[str, Any]] = [] # 다른 크롤러와의 일관성을 위해 rows에서 data로 변경

    def start_browser(self):
        """
        Chrome 브라우저를 실행 (Brave Browser 설정 포함)

        Returns:
            None: self.driver에 WebDriver 인스턴스 생성 후 저장
        """
        options = webdriver.ChromeOptions()
        
        # Brave Browser 바이너리 위치 설정 (필요 시 주석 해제하여 사용)
        # options.binary_location = "/Applications/Brave Browser.app/Contents/MacOS/Brave Browser"

        # CloudFlare Captcha 발생 시 Brave Browser를 사용하여 대응 가능함을 알림
        # options.add_argument("--headless=new") 
        options.add_argument("--disable-blink-features=AutomationControlled")
        options.add_argument("--disable-gpu")
        options.add_argument("--no-sandbox")
        options.add_argument("--disable-dev-shm-usage")
        options.add_argument("--window-size=1920,1080")
        
        # User-Agent 설정
        options.add_argument("user-agent=Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")
        
        self.driver = webdriver.Chrome(
            service=Service(ChromeDriverManager().install()),
            options=options
        )
        self.driver.set_page_load_timeout(300)
        self.driver.implicitly_wait(5)
    
    def scrape_reviews(self):
        """
        Letterboxd 리뷰 페이지에서 리뷰 데이터 수집
        페이지 로딩 실패에 대비하여 재시도 로직 포함
        'Older' 버튼 클릭을 통해 페이지 이동하며 수집
        목표 평점, 작성날짜, 리뷰 내용 수집

        Returns:
            None: 수집된 데이터는 self.data 리스트에 저장됨
        """
        self.start_browser()
        
        # 재시도를 포함한 초기 페이지 로드
        loaded = False
        for _ in range(3):
            try:
                self.driver.get(self.base_url)
                loaded = True
                break
            except Exception as e:
                try:
                    self.driver.quit()
                except:
                    pass
                self.start_browser()
        
        if not loaded:
            raise RuntimeError("여러 번의 시도 후에도 페이지 로드에 실패했습니다.")

        target = 1000
        page_count = 1
        
        while len(self.data) < target:
            # 1. 사람의 행동을 모방하기 위한 랜덤 지연
            time.sleep(random.uniform(2.0, 5.0))
            
            # 2. 현재 페이지에서 리뷰 추출
            try:
                cards = self.driver.find_elements(By.CSS_SELECTOR, "div.viewing-list li.film-detail, div.viewing-list article")
                print(f"Page {page_count}: {len(cards)}개의 리뷰 카드를 찾았습니다.")
                
                new_reviews_found = False
                for c in cards:
                    if len(self.data) >= target:
                        break
                        
                    rating_raw = ""
                    rating = ""
                    
                    try:
                        rating_element = c.find_element(By.CSS_SELECTOR, "span.rating")
                        rating_raw = rating_element.text.strip()
                    except:
                        # 별점이 없는 경우 대응
                        pass

                    if rating_raw:
                        stars = 0
                        half = 0
                        for ch in rating_raw:
                            if ch == "★":
                                stars += 1
                            elif ch == "½":
                                half = 1
                        rating = stars + (0.5 if half else 0)

                    date = ""
                    try:
                        t = c.find_element(By.CSS_SELECTOR, "time")
                        date = (t.get_attribute("datetime") or t.text).strip()
                    except:
                        pass

                    content = ""
                    try:
                        content = c.find_element(
                            By.CSS_SELECTOR,
                            "div.body-text, div.review, div.body, div.content, p"
                        ).text.strip()
                    except:
                        pass

                    if date and content:
                        self.data.append({
                            "rating": rating,
                            "rating_raw": rating_raw,
                            "date": date,
                            "content": content
                        })
                        new_reviews_found = True
                
                # 중간 저장
                if new_reviews_found:
                    print(f"현재까지 {len(self.data)}개의 리뷰를 수집했습니다...")
                    if len(self.data) % 100 == 0:
                        self.save_to_database()
                    
                if len(self.data) >= target:
                    break

                # 3. 페이지 탐색 (Older 버튼 클릭)
                try:
                    # "Older" 버튼 찾기
                    next_btn = self.driver.find_element(By.CSS_SELECTOR, "a.next")
                    if "Older" not in next_btn.text:
                        print("'Older' 버튼을 찾을 수 없습니다. (텍스트 불일치) 중단합니다.")
                        break
                        
                    # 버튼 위치로 스크롤
                    self.driver.execute_script("arguments[0].scrollIntoView(true);", next_btn)
                    time.sleep(1)
                    
                    # 클릭 (안정성을 위해 execute_script 사용)
                    self.driver.execute_script("arguments[0].click();", next_btn)
                    
                    page_count += 1
                    
                except Exception as e:
                    print(f"페이지 탐색 중 오류 발생 또는 마지막 페이지 도달: {e}")
                    break
                    
            except Exception as e:
                print(f"{page_count} 페이지에서 리뷰 추출 중 오류 발생: {e}")
                break

        print(f"크롤링 종료. 총 수집 리뷰 수: {len(self.data)}")
        self.driver.quit()
        self.driver = None

    def save_to_database(self):
        """
        수집한 리뷰 데이터를 CSV 파일로 저장

        Returns:
            None: output_dir 경로에 CSV 파일이 저장됨
        """
        if not self.data:
            print("저장할 데이터가 없습니다.")
            return

        if not os.path.exists(self.output_dir):
            os.makedirs(self.output_dir)
            
        filename = "reviews_letterboxd_newest.csv"
        filepath = os.path.join(self.output_dir, filename)

        df = pd.DataFrame(self.data)
        df.to_csv(filepath, index=False, encoding='utf-8-sig')
        print(f"수집된 {len(self.data)}개의 리뷰를 {filepath}에 저장했습니다.")
