from __future__ import annotations
from langdetect import detect, DetectorFactory
from langdetect.lang_detect_exception import LangDetectException
from nltk.corpus import stopwords
from nltk.stem import WordNetLemmatizer
import re
import os
import nltk
import pandas as pd
from review_analysis.preprocessing.base_processor import BaseDataProcessor

class LetterboxdProcessor(BaseDataProcessor):
    site_name = "letterboxd"
    def run(self) -> None:
        """
        Letterboxd 리뷰 데이터에 대한 전체 전처리 파이프라인을 실행한다.

        preprocess → feature_engineering → save_to_database 순서로
        전처리, 파생 변수 생성, 결과 저장을 수행한다.
        """
        print(f"[INFO] Start preprocessing: {self.site_name}")
        self.preprocess()
        self.feature_engineering()
        self.save_to_database()
        print(f"[INFO] Finished preprocessing: {self.site_name}")

    def __init__(self, input_path: str, output_path: str):
        super().__init__(input_path, output_path)
        self.df: pd.DataFrame | None = None

        self._stop_words: set[str] | None = None
        self._lemmatizer: WordNetLemmatizer | None = None
    
    def nltk_install(self) -> None:
        """
        NLTK 라이브러리 로컬에 없으면 다운로드하고 불용어, lemmatizer를 준비한다.
        """
        resources = ["stopwords", "wordnet", "omw-1.4"]
        for i in resources:
            try:
                nltk.data.find(f"corpora/{i}")
            except LookupError:
                nltk.download(i)

        if self._stop_words is None:
            self._stop_words = set(stopwords.words("english"))
        if self._lemmatizer is None:
            self._lemmatizer = WordNetLemmatizer()


    def clean_text(self, text: str) -> str:
        """
        review text를 분석에 적합하게 정제한다.
        
        Steps
        - 소문자 변환
        - 특수문자 제거 (알파벳과 공백만 유지)
        - 토큰화 (공백 기준 split)
        - 불용어 제거
        - Lemmatization (기본형/표제어로 변환)

        Args:
            text: 원본 리뷰 텍스트

        Returns:
            정제된 리뷰 텍스트 (clean_comment)
        """
        if not isinstance(text, str):
            return ""
        
        text = text.lower()
        text = re.sub(r"[^a-z\s]", " ", text)
        tokens = text.split()
        stop_words = set(stopwords.words("english"))
        tokens = [token for token in tokens if token not in stop_words]
        lemmatizer = WordNetLemmatizer()
        tokens = [lemmatizer.lemmatize(token) for token in tokens]

        return " ".join(tokens)
    

    def preprocess(self):
        """
        원본 CSV를 로드한 뒤 결측치/이상치/비영어 리뷰를 제거하고 clean_comment 및 date 파싱을 수행한다.
        결과는 self.df에 저장된다.
        """
        self.nltk_install()

        df = pd.read_csv(self.input_path)

        if "comment" not in df.columns and "content" in df.columns:
            df = df.rename(columns={"content": "comment"})

        df["comment"] = df["comment"].astype(str).str.strip()
        df = df[df["comment"] != ""]

        df = df.dropna(subset=["rating", "date", "comment"])

        df["raw_word_count"] = df["comment"].astype(str).str.split().str.len()

        df = df[df["raw_word_count"] >= 3]

        DetectorFactory.seed = 0

        def detect_language(text: str) -> str:
            """
            langdetect로 언어를 판별하고, 실패 시 unknown을 반환한다.
            """
            try:
                return detect(text)
            except LangDetectException:
                return "unknown"
        
        df["language"] = df["comment"].astype(str).apply(detect_language)

        df = df[df["language"] == "en"]

        df["clean_comment"] = df["comment"].apply(self.clean_text)

        df["date"] = pd.to_datetime(df["date"], errors="coerce")
        df = df.dropna(subset=["date"])

        self.df = df.reset_index(drop=True)


    def feature_engineering(self):
        """
        전처리된 데이터(self.df)에 파생 변수를 생성한다.

        - year_month: 월 단위(period) 변수
        - clean_word_count: 정제된 리뷰 단어 수
        :param self: 설명
        """
        if self.df is None:
            raise ValueError("Run preproces before feature enginieering.")

        df = self.df

        df["year_month"] = df["date"].dt.to_period("M")
        
        df["clean_word_count"] = df["clean_comment"].astype(str).str.split().str.len()

        self.df = df


    def save_to_database(self):
        """
        전처리, 파생변수까지 완료된 self.df를 CSV로 저장한다.
        """
        if self.df is None or self.df.empty:
            raise ValueError("No data to save. Run preprocess first.")
        
        os.makedirs(self.output_dir, exist_ok=True)

        base = os.path.splitext(os.path.basename(self.input_path))[0]
        site = base.replace("reviews_", "")

        filename = f"preprocessed_reviews_{site}.csv"
        out_file = os.path.join(self.output_dir, filename)

        self.df.to_csv(out_file, index=False, encoding="utf-8-sig")