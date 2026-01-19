from argparse import ArgumentParser
from typing import Dict, Type
import datetime
import re
import time
import os
import pandas as pd

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver import ActionChains
from selenium.webdriver.common.actions.wheel_input import ScrollOrigin
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager

from review_analysis.crawling.base_crawler import BaseCrawler
from utils.logger import setup_logger

class RottenTomatoesCrawler(BaseCrawler):
    """
    로튼 토마토(Rotten Tomatoes) 사이트에서 영화 '주토피아(Zootopia)'의 관람객 리뷰를 크롤링하는 클래스.

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
        self.data = []

    def start_browser(self):
        """
        크롬 웹드라이버를 설정하고 실행하는 메소드.

        Args:
            None
        
        Returns:
            None
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
            - date_str (str): 원본 날짜 문자열 (예: "Jan 8", "8h ago", "12/12/2024").
            - current_year (int): 현재 처리 중인 연도 (부분 날짜 파싱 시 사용).

        Returns:
            - (tuple): (파싱된 날짜 문자열, 업데이트된 연도) 튜플 반환.
        """
        today = datetime.datetime.now()
        
        try:
            date_str = date_str.strip()
            
            # Case 1: Relative hours ("8h ago")
            if 'h' in date_str:
                return today.strftime("%Y.%m.%d."), current_year
                
            # Case 2: Relative days ("6d ago")
            if 'd' in date_str:
                days_ago_match = re.search(r'(\d+)d', date_str)
                if days_ago_match:
                    days_ago = int(days_ago_match.group(1))
                    target_date = today - datetime.timedelta(days=days_ago)
                    return target_date.strftime("%Y.%m.%d."), current_year
            
            # Case 3: Full Date ("12/12/2024")
            if re.match(r'\d{1,2}/\d{1,2}/\d{4}', date_str):
                parsed_date = datetime.datetime.strptime(date_str, "%m/%d/%Y")
                return parsed_date.strftime("%Y.%m.%d."), parsed_date.year

            # Case 4: Partial Date ("Jan 8")
            # Remove "Verified" or other badges if present
            date_str = re.sub(r'Verified|Super Reviewer', '', date_str).strip()
            
            try:
                # Parse "Jan 8"
                parsed_date = datetime.datetime.strptime(date_str, "%b %d")
                final_date = parsed_date.replace(year=current_year)
                return final_date.strftime("%Y.%m.%d."), current_year
                
            except ValueError:
                return f"{current_year}.{date_str}", current_year # Fallback
                
        except Exception as e:
            self.logger.error(f"Error parsing date '{date_str}': {e}")
            return date_str, current_year

    def scrape_reviews(self):
        """
        로튼 토마토 사이트에서 리뷰를 크롤링하는 메인 로직을 수행하는 메소드. 
        'Load More' 버튼을 클릭하며 모든 리뷰를 수집함.

        Args:
            None

        Returns:
            None
        """
        self.start_browser()
        self.logger.info(f"Navigating to {self.url}")
        self.driver.get(self.url)
        time.sleep(3) # Wait for initial load

        # Handle Cookie Popup
        try:
            cookie_btn = self.driver.find_element(By.ID, "onetrust-accept-btn-handler")
            cookie_btn.click()
            self.logger.info("Clicked Cookie 'Continue' button")
            time.sleep(1)
        except:
            self.logger.info("No cookie popup found or handled")

        processed_indices = set()
        current_year = 2026
        last_month_num = 1
        
        while True:
            try:
                # Find all review cards
                reviews = self.driver.find_elements(By.CSS_SELECTOR, "review-card")
                new_reviews_found = False
                
                for i, review in enumerate(reviews):
                    if i in processed_indices:
                        continue
                    
                    try:
                        # Extract data
                        # Try multiple selectors for rating
                        try:
                            rating_element = review.find_element(By.CSS_SELECTOR, "[slot='rating']")
                            rating = rating_element.get_attribute("score") or rating_element.get_attribute("rating") or ""
                        except:
                            rating = "" # Some reviews might not have a rating
                        
                        try:
                            date_element = review.find_element(By.CSS_SELECTOR, "[slot='timestamp']")
                            raw_date = date_element.text
                            
                            # Heuristic to detect year change for partial dates (e.g. Jan -> Dec)
                            # Only applies if we are in partial date territory (no digits like /2016)
                            if "/" not in raw_date:
                                try:
                                    current_month = 1 # default
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

                                    # If we jumped from a low month (e.g. Jan=1) to a high month (e.g. Dec=12)
                                    if current_month > last_month_num + 6:
                                         current_year -= 1
                                         self.logger.info(f"Year decremented to {current_year}")
                                    
                                    last_month_num = current_month
                                except:
                                    pass

                            date, current_year = self.parse_date(raw_date, current_year)
                            
                        except Exception as e:
                            self.logger.error(f"Date error: {e}")
                            date = ""

                        try:
                            comment_element = review.find_element(By.CSS_SELECTOR, "[slot='content']")
                            comment = comment_element.text
                        except:
                            comment = ""
                        
                        if comment or rating: # Only save if there's real content
                            self.logger.info(f"Review {i+1}: {rating} | {date}")
                            self.data.append({
                                'rating': rating,
                                'date': date,
                                'comment': comment
                            })
                            new_reviews_found = True
                        
                        processed_indices.add(i)

                    except Exception as e:
                        self.logger.error(f"Error parsing review {i}: {e}")
                
                # Intermediate save
                if new_reviews_found and len(self.data) % 50 == 0:
                    self.save_to_database()

                # Find and Click 'Load More'
                try:
                    load_more_btn = None
                    try:
                         load_more_btn = self.driver.find_element(By.CSS_SELECTOR, "rt-button[data-pagemediareviewsmanager='loadMoreBtn:click']")
                    except:
                        # Fallback: check text content
                        buttons = self.driver.find_elements(By.TAG_NAME, "rt-button")
                        for btn in buttons:
                            if "Load More" in btn.text:
                                load_more_btn = btn
                                break
                    
                    if load_more_btn:
                        # Scroll to button
                        self.driver.execute_script("arguments[0].scrollIntoView(true);", load_more_btn)
                        time.sleep(1)
                        # Click
                        self.driver.execute_script("arguments[0].click();", load_more_btn)
                        self.logger.info("Clicked 'Load More'")
                        time.sleep(3) # Wait for new reviews to load
                    else:
                        self.logger.info("No 'Load More' button found.")
                        break
                except Exception as e:
                    self.logger.info(f"Load More error: {e}")
                    break

                # Safety break for testing (optional, remove if want ALL)
                # if len(self.data) >= 600:
                #     self.logger.info("Reached target review count.")
                #     break
                    
            except Exception as e:
                self.logger.error(f"Critical error in scraping loop: {e}")
                break

    def save_to_database(self):
        """
        수집된 리뷰 데이터를 CSV 파일로 저장하는 메소드.

        Args:
            None

        Returns:
            None
        """
        if not self.data:
            self.logger.warning("No data to save.")
            return

        if not os.path.exists(self.output_dir):
            os.makedirs(self.output_dir)
            
        filename = "reviews_RottenTomatoes.csv"
        filepath = os.path.join(self.output_dir, filename)
        
        df = pd.DataFrame(self.data)
        df.to_csv(filepath, index=False, encoding='utf-8-sig')
        self.logger.info(f"Saved {len(self.data)} reviews to {filepath}")

class LetterboxdCrawler(BaseCrawler):
    """
    Letterboxd 영화 리뷰 페이지에서 리뷰 데이터를 수집하는 크롤러
    리뷰의 별점, 작성 날짜, 리뷰 내용을 수집하여 csv 파일로 저장
    """
    def __init__(self, output_dir: str):
        """
        LetterboxdCrawler 객체 초기화

        Arg:
            output_dir: 수집한 리뷰 데이터 저장할 디렉토리 경로
        """
        super().__init__(output_dir)
        self.base_url = 'https://letterboxd.com/film/zootopia/reviews/'
        self.driver = None
        self.rows=[]

    def start_browser(self):
        """
        크롬 브라우저를 실행

        returns:
            self.driver: WebDriver 인스턴스 생성
        """
        options = webdriver.ChromeOptions()
        options.add_argument("--headless=new")
        options.add_argument("--disable-blink-features=AutomationControlled")
        options.add_argument("--disable-gpu")
        options.add_argument("--no-sandbox")
        options.add_argument("--disable-dev-shm-usage")
        options.add_argument("--window-size=1920,1080")
        self.driver = webdriver.Chrome(options=options)
        self.driver.set_page_load_timeout(300)
        self.driver.implicitly_wait(5)
    
    def scrape_reviews(self):
        """
        Letterboxd 사이트에서 리뷰 데이터 수집
        페이지 로딩 실패에 대비하여 재시도 로직 포함
        'Older'버튼 클릭 시 페이지 이동
        최대 500개 리뷰 수집 후 수집 멈춤

        returns:
            self.row: 수집된 리뷰 데이터 저장
        """
        self.start_browser()
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

        target = 500
        page_turn = 0
        while True:
            try:
                cards = self.driver.find_elements("css selector", "div.viewing-list li, div.viewing-list article")
            except:
           
                try:
                    self.driver.quit()
                except:
                    pass
                self.start_browser()
                self.driver.get(self.base_url)
                continue

            
            for c in cards:
                rating_raw=""
                rating = ""
                
                try:
                    rating_raw = c.find_element("css selector", "span.rating").text.strip()
                except:
                    try:
                        first = (c.text or "").strip().split()[0]
                        if "★" in first:
                            rating_raw = first
                    except:
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
                    t = c.find_element("css selector", "time")
                    date = (t.get_attribute("datetime") or t.text).strip()
                except:
                    pass

                content = ""
                try:
                    content = c.find_element(
                        "css selector",
                        "div.body-text, div.review, div.body, div.content, p"
                    ).text.strip()
                except:
                    pass

                if date and content:
                    self.rows.append({
                        "rating": rating,
                        "rating_raw": rating_raw,
                        "date": date,
                        "content": content
                    })

                    if len(self.rows) >= target:
                        break

            if len(self.rows) >= target:
                break

            try:
                self.driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
                self.driver.find_element("link text", "Older").click()
            except:
                break

        self.driver.quit()
        self.driver = None

    def save_to_database(self):
        """
        수집한 데이터를 reviews_letterboxd.csv 파일로 저장

        returns:
            csv 파일이 ouput_dir 경로에 저장
        """
        os.makedirs(self.output_dir, exist_ok=True)

        path = f"{self.output_dir}/reviews_letterboxd.csv"

        with open(path, "w", encoding="utf-8") as f:
            f.write("rating,rating_raw,date,content\n")
            for r in self.rows:
                content = r["content"].replace("\n", " ").replace('"', '""')
                date = r["date"].replace("\n", " ").replace('"', '""')
                rating_raw = r["rating_raw"].replace("\n", " ").replace('"', '""')
                f.write(f'{r["rating"]},"{rating_raw}","{date}","{content}"\n')

class IMDbCrawler(BaseCrawler):
    """
    Collects reviews for Zootopia (2016) from IMDb and saves them as a CSV file,
    including ratings, dates, and review content.
    """
    def __init__(self, output_dir: str):
        super().__init__(output_dir)
        self.logger = logging.getLogger(__name__)
        self.base_url = 'https://www.imdb.com/title/tt2948356/reviews/'

        
    def start_browser(self):
        """
        Starts the browser and navigates to the IMDb reviews page.
        """
        self.driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=Options())
        self.driver.get(self.base_url)
        self.logger.info("브라우저 실행 완료.")
        time.sleep(2)
        

    def load_until_target(self, target: int = 500, max_no_growth: int = 5, timeout: int = 10) -> int:
        """
        Repeatedly clicks the "Load More" button on the IMDb reviews page to load reviews
        up to the specified target count, verifying actual increases in review cards.

        Returns:
            int: The final number of loaded review cards.
        """
        wait = WebDriverWait(self.driver, timeout)

        def count_cards() -> int:
            """
            Counts the number of review cards currently loaded on the IMDb reviews page.

            Returns:
                int: The number of review card elements found in the DOM.
            """
            return len(self.driver.find_elements(By.CSS_SELECTOR, 'div[data-testid="review-card-parent"]'))

        no_growth = 0
        last_count = count_cards()

        try:
            wait.until(lambda d: count_cards() > 0)
        except TimeoutException:
            return 0

        while last_count < target and no_growth < max_no_growth:
            self.driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")

            load_more = None
            selectors = [
                (By.CSS_SELECTOR, "button.ipc-see-more__button"),
                (By.XPATH, "//button[contains(@class,'ipc-see-more__button')]"),
            ]

            for by, sel in selectors:
                try:
                    load_more = wait.until(EC.presence_of_element_located((by, sel)))
                    break
                except TimeoutException:
                    continue

            if load_more is None:
                break

            try:
                self.driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", load_more)
                time.sleep(0.3)
                try:
                    load_more.click()
                except (StaleElementReferenceException, Exception):
                    self.driver.execute_script("arguments[0].click();", load_more)
            except Exception:
                break

            try:
                wait.until(lambda d: count_cards() > last_count)
                new_count = count_cards()
                last_count = new_count
                no_growth = 0
            except TimeoutException:
                no_growth += 1

        return last_count


    def scrape_reviews(self):
        """
        Scrapes reviews from the IMDb reviews page.

        This method loads reviews by interacting with the "Load More" button,
        extracts review ratings, dates, and content from the page,
        and stores the collected data in a pandas DataFrame.

        The scraping process stops once the target number of valid reviews is collected.
        """
        self.start_browser()

        target_save = 500
        target_load = 1000

        loaded = self.load_until_target(target=target_load, max_no_growth=6, timeout=10)
        self.logger.info(f"로드된 리뷰 카드 수: {loaded}")

        soup = BeautifulSoup(self.driver.page_source, "html.parser")

        items = soup.select('div[data-testid="review-card-parent"]')
        dates = soup.select('div[data-testid="reviews-author"] li.review-date')

        self.logger.info(f"items={len(items)}, dates={len(dates)}")

        reviews = []

        n = min(len(items), len(dates))
        for i in range(n):
            item = items[i]

            # date
            date = dates[i].get_text(strip=True)

            # rating
            rating = None
            rating_tag = item.select_one('span[aria-label*="rating"]') or item.select_one("span.review-rating")
            if rating_tag and rating_tag.has_attr("aria-label"):
                m = re.search(r"rating:\s*([\d.]+)", rating_tag["aria-label"])
                if not m:
                    m = re.search(r"\d+(\.\d+)?", rating_tag["aria-label"])
                rating = m.group(1) if (m and m.lastindex) else (m.group() if m else None)

            # content
            content_tag = item.select_one('div[data-testid="review-overflow"]') or item.select_one("div.ipc-overflowText")
            content = content_tag.get_text(strip=True) if content_tag else None

            if rating and date and content:
                reviews.append({"rating": rating, "date": date, "content": content})

            if len(reviews) >= target_save:
                break

        self.data = pd.DataFrame(reviews)
        self.logger.info(f"최종 수집: {len(self.data)}개")


    def save_to_database(self):
        """
        Saves the collected review data to a CSV file.
        """
        self.logger.info("save_to_database() called")

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


# 모든 크롤링 클래스를 예시 형식으로 적어주세요. 
CRAWLER_CLASSES: Dict[str, Type[BaseCrawler]] = {
#    "naver_movie": NaverMovieCrawler,
    "rotten_tomatoes": RottenTomatoesCrawler,
    "letterboxd": LetterboxdCrawler,
    "imdb": IMDbCrawler,
}

def create_parser() -> ArgumentParser:
    parser = ArgumentParser()
    parser.add_argument('-o', '--output_dir', type=str, required=True, help="Output file directory. Example: ../../database")
    parser.add_argument('-c', '--crawler', type=str, required=False, choices=CRAWLER_CLASSES.keys(),
                        help=f"Which crawler to use. Choices: {', '.join(CRAWLER_CLASSES.keys())}")
    parser.add_argument('-a', '--all', action='store_true', 
                        help="Run all crawlers. Default to False.")    
    return parser

if __name__ == "__main__":
    parser = create_parser()
    args = parser.parse_args()

    if args.all: 
        for crawler_name in CRAWLER_CLASSES.keys():
            Crawler_class = CRAWLER_CLASSES[crawler_name]
            crawler = Crawler_class(args.output_dir)
            crawler.scrape_reviews()
            crawler.save_to_database()
     
    elif args.crawler:
        Crawler_class = CRAWLER_CLASSES[args.crawler]
        crawler = Crawler_class(args.output_dir)
        crawler.scrape_reviews()
        crawler.save_to_database()
    
    else:
        raise ValueError("No crawlers.")