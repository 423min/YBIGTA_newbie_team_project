import os
import sys
from argparse import ArgumentParser
from typing import Dict, Type

# Project root 추가하여 실행 시 파일 경로 문제 해결
current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.dirname(os.path.dirname(current_dir))
if project_root not in sys.path:
    sys.path.append(project_root)

from review_analysis.crawling.base_crawler import BaseCrawler
from review_analysis.crawling.rotten_tomatoes_crawler import RottenTomatoesCrawler
from review_analysis.crawling.letterboxd_crawler import LetterboxdCrawler
from review_analysis.crawling.imdb_crawler import IMDbCrawler

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