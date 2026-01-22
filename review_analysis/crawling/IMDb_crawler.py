import logging
import re
import time
import datetime
import pandas as pd  # type: ignore[import-untyped]
from bs4 import BeautifulSoup  # type: ignore[import-untyped]
from selenium import webdriver
from selenium.common.exceptions import StaleElementReferenceException, TimeoutException
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait
from webdriver_manager.chrome import ChromeDriverManager
from review_analysis.crawling.base_crawler import BaseCrawler



class IMDbCrawler(BaseCrawler):
    """
    IMDb 사이트에서 영화 '주토피아(Zootopia)'의 리뷰를 수집하여 CSV 파일로 저장하는 클래스.
    평점, 작성일, 리뷰 내용을 포함합니다.

    Attributes:
        - logger (logging.Logger): 로깅을 위한 로거 인스턴스.
        - base_url (str): 크롤링 대상인 IMDb 리뷰 페이지 URL.
        - driver (webdriver.Chrome): Selenium 웹드라이버 인스턴스.
    
    Methods:
        - start_browser: 브라우저를 실행하고 대상 URL로 이동.
        - load_until_target: 'Load More' 버튼을 클릭하여 목표 개수만큼 리뷰를 로드.
        - scrape_reviews: 로드된 페이지에서 리뷰 데이터를 추출.
        - save_to_database: 수집된 데이터를 CSV 파일로 저장.
    """
    def __init__(self, output_dir: str):
        super().__init__(output_dir)
        self.logger = logging.getLogger(__name__)
        self.base_url = 'https://www.imdb.com/title/tt2948356/reviews/'

        
    def start_browser(self):
        """
        브라우저를 시작하고 IMDb 리뷰 페이지로 이동합니다.
        """
        self.driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=Options())
        self.driver.get(self.base_url)
        self.logger.info("브라우저 실행 완료.")
        time.sleep(2)
        

    def load_until_target(self, target: int = 500, max_no_growth: int = 5, timeout: int = 10) -> int:
        """
        IMDb 리뷰 페이지의 'Load More' 버튼을 반복적으로 클릭하여 
        지정된 목표 개수까지 리뷰를 로드하고, 실제 카드 수가 증가하는지 확인합니다.

        Args:
            target: 로드하고자 하는 목표 리뷰 수.
            max_no_growth: 데이터 증가가 없을 때 최대 시도 횟수.
            timeout: 각 대기 단계에서의 타임아웃 시간.

        Returns:
            int: 최종적으로 로드된 리뷰 카드의 수.
        """
        wait = WebDriverWait(self.driver, timeout)

        def count_cards() -> int:
            """
            현재 IMDb 리뷰 페이지에 로드된 리뷰 카드의 개수를 셉니다.

            Returns:
                int: DOM 내에서 발견된 리뷰 카드 요소의 개수.
            """
            return len(self.driver.find_elements(By.CSS_SELECTOR, 'div[data-testid="review-card-parent"]'))

        no_growth = 0
        last_count = count_cards()

        try:
            wait.until(lambda d: count_cards() > 0)
        except TimeoutException:
            return 0

        while last_count < target and no_growth < max_no_growth:
            # 페이지 하단으로 스크롤
            self.driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")

            load_more = None
            selectors = [
                (By.CSS_SELECTOR, "button.ipc-see-more__button"),
                (By.XPATH, "//button[contains(@class,'ipc-see-more__button')]"),
            ]

            # 'Load More' 버튼 찾기
            for by, sel in selectors:
                try:
                    load_more = wait.until(EC.presence_of_element_located((by, sel)))
                    break
                except TimeoutException:
                    continue

            if load_more is None:
                break

            try:
                # 버튼이 화면 중앙에 오도록 스크롤 후 클릭
                self.driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", load_more)
                time.sleep(0.3)
                try:
                    load_more.click()
                except (StaleElementReferenceException, Exception):
                    self.driver.execute_script("arguments[0].click();", load_more)
            except Exception:
                break

            try:
                # 새로운 카드가 로드될 때까지 대기
                wait.until(lambda d: count_cards() > last_count)
                new_count = count_cards()
                last_count = new_count
                no_growth = 0
            except TimeoutException:
                no_growth += 1

        return last_count


    def scrape_reviews(self):
        """
        IMDb 리뷰 페이지에서 리뷰 데이터를 크롤링합니다.

        'Load More' 버튼을 조작하여 리뷰를 충분히 로드한 뒤,
        페이지 내에서 평점, 날짜, 내용을 추출하여 pandas DataFrame으로 저장합니다.

        목표 개수의 유효한 리뷰가 수집되면 중단합니다.
        """
        self.start_browser()

        target_save = 1000
        target_load = 2000

        loaded = self.load_until_target(target=target_load, max_no_growth=6, timeout=10)
        self.logger.info(f"로드된 리뷰 카드 수: {loaded}")

        # BeautifulSoup을 사용하여 로드된 HTML 파싱
        soup = BeautifulSoup(self.driver.page_source, "html.parser")

        items = soup.select('div[data-testid="review-card-parent"]')
        dates = soup.select('div[data-testid="reviews-author"] li.review-date')

        self.logger.info(f"items={len(items)}, dates={len(dates)}")

        reviews = []

        n = min(len(items), len(dates))
        for i in range(n):
            item = items[i]

            # 날짜 추출
            date = dates[i].get_text(strip=True)

            # 평점 추출
            rating = None
            rating_tag = item.select_one('span[aria-label*="rating"]') or item.select_one("span.review-rating")
            if rating_tag and rating_tag.has_attr("aria-label"):
                m = re.search(r"rating:\s*([\d.]+)", rating_tag["aria-label"])
                if not m:
                    m = re.search(r"\d+(\.\d+)?", rating_tag["aria-label"])
                rating = m.group(1) if (m and m.lastindex) else (m.group() if m else None)

            # 내용 추출
            content_tag = item.select_one('div[data-testid="review-overflow"]') or item.select_one("div.ipc-overflowText")
            content = content_tag.get_text(strip=True) if content_tag else None

            if rating and date and content:
                try:
                    rating_f = float(rating)
                except Exception:
                    rating_f = None
                reviews.append({"rating": rating_f, "date": date, "comment": content})

            if len(reviews) >= target_save:
                break

        self.data = pd.DataFrame(reviews)

        def _parse_imdb_date(s: str) -> datetime.datetime | None:
            """
            다양한 형식의 날짜 문자열을 datetime 객체로 변환을 시도합니다.
            실패 시 None을 반환합니다.
            """
            s = (s or "").strip()
            for fmt in ("%d %B %Y", "%B %d, %Y", "%d %b %Y", "%b %d, %Y"):
                try:
                    return datetime.datetime.strptime(s, fmt)
                except ValueError:
                    continue
            return None
               
        # 날짜 형식 변환 및 정렬
        self.data["date_dt"] = self.data["date"].apply(_parse_imdb_date)
        self.data = self.data.dropna(subset=["date_dt"])
        self.data = self.data.sort_values("date_dt", ascending=False)
        self.data["date"] = self.data["date_dt"].dt.strftime("%Y.%m.%d.")
        self.data = self.data[["rating", "date", "comment"]]
        self.logger.info(f"최종 수집: {len(self.data)}개")


    def save_to_database(self):
        """
        수집된 리뷰 데이터를 CSV 파일로 저장합니다.
        """
        self.logger.info("save_to_database() 호출됨")

        if getattr(self, "data", None) is None or self.data.empty:
            self.logger.warning("저장할 데이터가 없습니다. (self.data가 비어있음)")
            if self.driver:
                self.driver.quit()
            return

        output_path = f"{self.output_dir}/reviews_imdb.csv"
        self.data.to_csv(output_path, index=False, encoding='utf-8-sig')
        self.logger.info(f"저장 완료: {output_path}")

        if self.driver:
            self.driver.quit()