from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
import time
import random
import os
import pandas as pd
from review_analysis.crawling.base_crawler import BaseCrawler

from typing import List, Dict, Any

class LetterboxdCrawler(BaseCrawler):
    """
    Letterboxd 영화 리뷰 사이트에서 리뷰 데이터를 수집
    """
    def __init__(self, output_dir: str):
        """
        LetterboxdCrawler 객체 초기화

        Arg:
            output_dir: 수집한 리뷰 데이터를 저장할 디렉퇼 경로
        """
        super().__init__(output_dir)
        self.base_url = 'https://letterboxd.com/film/zootopia/reviews/'
        self.driver = None
        self.data: List[Dict[str, Any]] = [] 

    def start_browser(self):
        """
        Chorme 브라우저를 실행 (Brave Browser 사용)

        returns:
            self.driver=WebDriver 인스턴스 생성 후 저장
        """
        options = webdriver.ChromeOptions()
        
        # Set Brave Browser binary location
        #options.binary_location = "/Applications/Brave Browser.app/Contents/MacOS/Brave Browser"

        # Note: 가끔씩 CloudFlare Captcha가 발생할 수 있음
        # 이 경우 Brave Browser를 사용하여 크롤링을 할 수 있음

        # Remove headless for better bot avoidance or debugging if needed, but keeping user config
        # options.add_argument("--headless=new") 
        options.add_argument("--disable-blink-features=AutomationControlled")
        options.add_argument("--disable-gpu")
        options.add_argument("--no-sandbox")
        options.add_argument("--disable-dev-shm-usage")
        options.add_argument("--window-size=1920,1080")
        
        # Add user agent
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
        'Older' 버튼 클릭을 통해서 페이지 이동
        500개의 리뷰 수집 후 크롤링 끝
        별점, 작성날짜, 리뷰 내용 수집

        returns:
            self.data=수집된 리뷰 데이터 저장 리스트
        """
        self.start_browser()
        
        # Initial load with retry
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
            raise RuntimeError("Page load failed after retries")

        target = 1000
        page_count = 1
        
        while len(self.data) < target:
            # 1. Random delay to mimic human behavior
            time.sleep(random.uniform(2.0, 5.0))
            
            # 2. Extract reviews from current page
            try:
                cards = self.driver.find_elements(By.CSS_SELECTOR, "div.viewing-list li.film-detail, div.viewing-list article")
                print(f"Page {page_count}: Found {len(cards)} cards")
                
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
                        # Sometimes rating is just text at start? Fallback logic
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
                
                # Intermediate save
                if new_reviews_found:
                    print(f"Collected {len(self.data)} reviews so far...")
                    if len(self.data) % 100 == 0:
                        self.save_to_database()
                    
                if len(self.data) >= target:
                    break

                # 3. Navigation
                try:
                    # Look for "Older/Newer" button
                    next_btn = self.driver.find_element(By.CSS_SELECTOR, "a.next")
                    if "Older" not in next_btn.text:
                        print("No 'Older' button found (text mismatch). Stopping.")
                        break

                    # if "Newer" not in next_btn.text:
                    #     print("No 'Newer' button found (text mismatch). Stopping.")
                    #     break
                        
                    # Scroll to button
                    self.driver.execute_script("arguments[0].scrollIntoView(true);", next_btn)
                    time.sleep(1)
                    
                    # Click
                    # Use execute_script for reliability
                    self.driver.execute_script("arguments[0].click();", next_btn)
                    
                    # Wait for URL change or old elements to become stale happens implicitly by next find_elements
                    # But explicit wait is better
                    page_count += 1
                    
                except Exception as e:
                    print(f"Navigation error or end of pages: {e}")
                    break
                    
            except Exception as e:
                print(f"Error extracting reviews on page {page_count}: {e}")
                break

        print(f"Finished. Total reviews: {len(self.data)}")
        self.driver.quit()
        self.driver = None

    def save_to_database(self):
        """
        수집한 리뷰 데이터 csv 파일로 저장
        
        returns:
            csv 파일이 output_dir 경로에 저장
        """
        if not self.data:
            print("No data to save.")
            return

        if not os.path.exists(self.output_dir):
            os.makedirs(self.output_dir)
        # filename = "reviews_letterboxd_oldest.csv"
        filename = "reviews_letterboxd_newest.csv"
        filepath = os.path.join(self.output_dir, filename)

        df = pd.DataFrame(self.data)
        df.to_csv(filepath, index=False, encoding='utf-8-sig')
        print(f"Saved {len(self.data)} reviews to {filepath}")
