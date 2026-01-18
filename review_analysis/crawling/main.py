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

class NaverMovieCrawler(BaseCrawler):
    def __init__(self, output_dir: str):
        super().__init__(output_dir)
        self.logger = setup_logger()
        self.url = 'https://search.naver.com/search.naver?where=nexearch&sm=tab_etc&mra=bkEw&pkid=68&os=2085555&qvt=0&query=%EC%98%81%ED%99%94%20%EC%A3%BC%ED%86%A0%ED%94%BC%EC%95%84%20%EA%B4%80%EB%9E%8C%ED%8F%89'
        self.driver = None
        self.data = []

    def start_browser(self):
        chrome_options = webdriver.ChromeOptions()
        chrome_options.add_experimental_option("detach", True)
        chrome_options.add_experimental_option("excludeSwitches", ["enable-logging"])
        
        self.driver = webdriver.Chrome(
            service=Service(ChromeDriverManager().install()),
            options=chrome_options
        )
        self.driver.maximize_window()

    def scrape_reviews(self):
        self.start_browser()
        self.logger.info(f"Navigating to {self.url}")
        self.driver.get(self.url)
        time.sleep(2)

        # 스포일러 감상 보여 주기 버튼 클릭
        try:
            spoiler_btn = self.driver.find_element(By.XPATH, "//button[contains(@class, 'btn_area_auto') and @title='스포일러']")
            spoiler_btn.click()
            self.logger.info("Clicked spoiler button")
        except:
            self.logger.info("No spoiler button found or already expanded")

        comment_index = 1
        nonsave_number = 0
        
        self.logger.info("Starting review crawl...")
        
        while True:
            try:
                base_xpath = f"//div[contains(@class, 'lego_review_list')]//li[{comment_index}]"
                
                # Check existence of the item first
                raw_item = self.driver.find_elements(By.XPATH, base_xpath)
                if len(raw_item) == 0:
                     # Attempt to scroll down to load more
                    try:
                        # Find the scrollable container
                        scroller = self.driver.find_element(By.CSS_SELECTOR, ".lego_review_list._scroller")
                        
                        # Use JavaScript to scroll to the bottom of this container
                        self.driver.execute_script("arguments[0].scrollTop = arguments[0].scrollHeight;", scroller)
                        
                        time.sleep(1) # Wait for load
                        
                         # Check again
                        raw_item = self.driver.find_elements(By.XPATH, base_xpath)
                        if len(raw_item) == 0:
                            self.logger.info(f"No more reviews found at index {comment_index}. Stopping crawling.")
                            break
                    except Exception as e:
                        self.logger.error(f"Error during scrolling: {e}")
                        break

                # Comment
                raw_comment = self.driver.find_elements(By.XPATH, f"{base_xpath}//span[contains(@class, 'desc') and contains(@class, '_text')]")
                comment = raw_comment[0].text if raw_comment else ""

                # Rating
                raw_rating = self.driver.find_elements(By.XPATH, f"{base_xpath}//div[contains(@class, 'area_text_box')]")
                rating = raw_rating[0].text if raw_rating else ""
                
                # Date
                raw_date = self.driver.find_elements(By.XPATH, f"{base_xpath}//dl[contains(@class, 'cm_upload_info')]//dd[contains(@class, 'this_text_normal')]")
                date = raw_date[0].text if raw_date else ""
                
                if comment:
                    self.logger.info(f"Review {comment_index}: {rating} | {date}")
                    self.data.append({
                        'rating': rating,
                        'date': date,
                        'comment': comment
                    })
                    
                    # Intermediate save every 50 reviews
                    if len(self.data) % 50 == 0:
                        self.save_to_database()
                else:
                    nonsave_number += 1
                
                comment_index += 1
                
            except Exception as e:
                self.logger.error(f"Error processing review {comment_index}: {e}")
                break

    def save_to_database(self):
        if not self.data:
            self.logger.warning("No data to save.")
            return

        if not os.path.exists(self.output_dir):
            os.makedirs(self.output_dir)
            
        filename = "reviews_NaverMovie.csv"
        filepath = os.path.join(self.output_dir, filename)
        
        df = pd.DataFrame(self.data)
        df.to_csv(filepath, index=False, encoding='utf-8-sig')
        self.logger.info(f"Saved {len(self.data)} reviews to {filepath}")

class RottenTomatoesCrawler(BaseCrawler):
    def __init__(self, output_dir: str):
        super().__init__(output_dir)
        self.logger = setup_logger()
        self.url = "https://www.rottentomatoes.com/m/zootopia/reviews/all-audience"
        self.driver = None
        self.data = []

    def start_browser(self):
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
        Parses date string into YYYY-MM-DD format.
        Handles:
        - Relative time: "8h ago", "6d ago" -> Calculate from current date (2026-01-19)
        - Partial date: "Jan 8", "Dec 25" -> Append year and format
        """
        today = datetime.datetime(2026, 1, 19) # Fixed current date as per user context
        
        try:
            date_str = date_str.strip()
            
            # Case 1: Relative hours ("8h ago")
            if 'h' in date_str:
                return today.strftime("%Y.%m.%d."), current_year
                
            # Case 2: Relative days ("6d ago")
            if 'd' in date_str:
                days_ago = int(re.search(r'(\d+)d', date_str).group(1))
                target_date = today - datetime.timedelta(days=days_ago)
                # If target date moved to previous year (e.g. Jan 1 -> Dec 31)
                if target_date.year < 2026:
                    # This logic handles small overlaps, but the main year logic is below for "Jan 8" style
                    pass 
                return target_date.strftime("%Y.%m.%d."), current_year

            # Case 3: Partial Date ("Jan 8")
            # Remove "Verified" or other badges if present
            date_str = re.sub(r'Verified|Super Reviewer', '', date_str).strip()
            
            try:
                # Parse "Jan 8"
                parsed_date = datetime.datetime.strptime(date_str, "%b %d")
                
                # Check if we moved back a year
                # Logic: If the new month is greater than the previous month (e.g. we were processing Jan, now we see Dec),
                # it means we crossed the year boundary.
                # HOWEVER: We don't have the *previous* month easily available here without state.
                # So we simply assign the `current_year`. The caller is responsible for updating `current_year`
                # if they detect a month jump (e.g. going from Jan items to Dec items).
                
                final_date = parsed_date.replace(year=current_year)
                return final_date.strftime("%Y.%m.%d."), current_year
                
            except ValueError:
                return f"{current_year}.{date_str}", current_year # Fallback
                
        except Exception as e:
            self.logger.error(f"Error parsing date '{date_str}': {e}")
            return date_str, current_year

    def scrape_reviews(self):
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
                            date = date_element.text #del
                            # raw_date = date_element.text
                            
                            # # Update Year Logic
                            # # If we parse a date like "Dec 31" and we were previously at "Jan 1",
                            # # we need to decrement the year.
                            # # We can infer month from raw_date
                            # try:
                            #     if "Jan" in raw_date: current_month = 1
                            #     elif "Feb" in raw_date: current_month = 2
                            #     elif "Mar" in raw_date: current_month = 3
                            #     elif "Apr" in raw_date: current_month = 4
                            #     elif "May" in raw_date: current_month = 5
                            #     elif "Jun" in raw_date: current_month = 6
                            #     elif "Jul" in raw_date: current_month = 7
                            #     elif "Aug" in raw_date: current_month = 8
                            #     elif "Sep" in raw_date: current_month = 9
                            #     elif "Oct" in raw_date: current_month = 10
                            #     elif "Nov" in raw_date: current_month = 11
                            #     elif "Dec" in raw_date: current_month = 12
                            #     else: current_month = last_month_num # Fallback or relative time

                            #     # If we jumped from a low month (e.g. Jan=1) to a high month (e.g. Dec=12), it's a new year backward
                            #     if current_month > last_month_num + 6: # Heuristic: if jump is large positive (1 -> 12 is +11)
                            #          current_year -= 1
                            #          self.logger.info(f"Year decremented to {current_year}")
                                
                            #     last_month_num = current_month
                        except:
                            pass

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

# 모든 크롤링 클래스를 예시 형식으로 적어주세요. 
CRAWLER_CLASSES: Dict[str, Type[BaseCrawler]] = {
    "naver_movie": NaverMovieCrawler,
    "rotten_tomatoes": RottenTomatoesCrawler,
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