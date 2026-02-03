from __future__ import annotations
from langdetect import detect, DetectorFactory # type: ignore
from langdetect.lang_detect_exception import LangDetectException # type: ignore
from nltk.corpus import stopwords # type: ignore
from nltk.stem import WordNetLemmatizer # type: ignore
import re
import os
import nltk # type: ignore
import pandas as pd # type: ignore
from review_analysis.preprocessing.base_processor import BaseDataProcessor

from review_analysis.preprocessing.lexicon_loader import load_lexicon
from sklearn.feature_extraction.text import TfidfVectorizer # type: ignore
import numpy as np

class LetterboxdProcessor(BaseDataProcessor):
    """
    Letterboxd 리뷰 데이터를 위한 전처리 및 파생 변수 생성 클래스입니다.
    """
    site_name = "letterboxd"

    def run(self) -> None:
        """
        Letterboxd 리뷰 데이터에 대한 전체 전처리 파이프라인을 실행합니다.
        preprocess → feature_engineering → save_to_database 순서로 진행됩니다.
        """
        print(f"[정보] {self.site_name} 전처리 시작")
        self.preprocess()
        self.feature_engineering()
        self.save_to_database()
        print(f"[정보] {self.site_name} 전처리 완료")

    def __init__(self, input_path: str, output_path: str):
        """
        LetterboxdProcessor 초기화

        Args:
            input_path: 원본 CSV 파일 경로
            output_path: 결과 저장 디렉토리
        """
        super().__init__(input_path, output_path)
        self.df: pd.DataFrame | None = None

        self._stop_words: set[str] | None = None
        self._lemmatizer: WordNetLemmatizer | None = None
    
    def nltk_install(self) -> None:
        """
        필요한 NLTK 리소스를 확인하고 없으면 다운로드합니다.
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
        리뷰 텍스트를 분석에 적합하게 정제합니다.
        소문자 변환, 특수문자 제거, 불용어 제거, 어간 추출을 수행합니다.

        Args:
            text: 원본 리뷰 텍스트

        Returns:
            str: 정제된 리뷰 텍스트
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
    

    def preprocess(self, df: pd.DataFrame | None = None):
        """
        원본 데이터를 로드하여 결측치 처리, 스포일러 방지 문구 제거, 단어 수 필터링, 영어 리뷰 추출 등을 수행합니다.
        """
        self.nltk_install()

        if df is None:
            df = pd.read_csv(self.input_path)
        else:
            df = df.copy()

        if "comment" not in df.columns and "content" in df.columns:
            df = df.rename(columns={"content": "comment"})

        df["comment"] = df["comment"].astype(str).str.strip()
        df = df[df["comment"] != ""]

        df = df.dropna(subset=["rating", "date", "comment"])

        # "spoiler shield" 리뷰는 이상치로 처리해서 제거
        spoiler_text = "This review may contain spoilers. I can handle the truth."
        df = df[~df["comment"].astype(str).str.contains(spoiler_text, regex=False)]

        df["raw_word_count"] = df["comment"].astype(str).str.split().str.len()

        df = df[df["raw_word_count"] >= 3]

        DetectorFactory.seed = 0

        def detect_language(text: str) -> str:
            """
            텍스트의 언어를 판별합니다.
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
        전처리된 데이터에 파생 변수를 생성합니다.
        """
        if self.df is None:
            raise ValueError("feature_engineering 실행 전 preprocess를 먼저 실행해야 합니다.")

        df = self.df

        df["year_month"] = df["date"].dt.to_period("M")
        
        df["clean_word_count"] = df["clean_comment"].astype(str).str.split().str.len()

        self.df = df
        
        self.add_subjectivity_score()

    def add_subjectivity_score(self):
        """
        TF-IDF와 감정 사전을 결합하여 각 리뷰의 주관성 점수를 계산합니다.

        주관성 점수 산출 방식:
        1. 어휘 사전 구축: MPQA Subjectivity Lexicon을 기반으로 영화 리뷰 도메인에 적합한 단어들을 필터링하고 강도(Strong/Very Strong)별 가중치 부여
        2. TF-IDF 가중치: L2 정규화를 비활성화하여 단어의 코퍼스 내 희소성(IDF)과 리뷰 내 빈도(TF)를 보존한 절대적 중요도 산출
        3. 계산식: Sum(단어별 TF-IDF 값 * 사전 가중치) / 정제된 리뷰 단어 수 (리뷰 길이에 따른 편향 제거)
        """
        if self.df is None:
            raise ValueError("주관성 점수 계산 전 preprocess를 먼저 실행해야 합니다.")
            
        df = self.df
        
        # 어휘 사전 로드
        lexicon = load_lexicon()
        if not lexicon:
            print("[경고] 어휘 사전이 비어 있습니다. 모든 주관성 점수가 0으로 설정됩니다.")
            df["subjectivity_score"] = 0.0
            self.df = df
            return

        # TF-IDF 초기화
        vectorizer = TfidfVectorizer(token_pattern=r"(?u)\b\w+\b", norm=None)
        
        try:
            # TF-IDF 행렬 생성
            tfidf_matrix = vectorizer.fit_transform(df["clean_comment"].astype(str))
            
            # 피처 이름 목록 가져오기
            feature_names = vectorizer.get_feature_names_out()
            
            # 피처 순서에 맞춰 가중치 벡터 생성
            weights = []
            for word in feature_names:
                lex_weight = lexicon.get(word)
                if lex_weight:
                    weights.append(lex_weight)
                else:
                    weights.append(0.5)
            weights = np.array(weights)
            
            # 점수 계산
            raw_scores = tfidf_matrix.dot(weights)
            
            # 단어 수로 정규화
            word_counts = df["clean_word_count"].replace(0, 1).values
            
            df["subjectivity_score"] = raw_scores / word_counts
            
        except ValueError as e:
            print(f"[경고] TF-IDF 계산 중 오류 발생: {e}. 점수를 0으로 설정합니다.")
            df["subjectivity_score"] = 0.0

        self.df = df


    def save_to_database(self):
        """
        전처리 및 분석이 완료된 데이터를 CSV 파일로 저장합니다.
        """
        if self.df is None or self.df.empty:
            raise ValueError("저장할 데이터가 없습니다. 먼저 preprocess를 실행하세요.")
        
        os.makedirs(self.output_dir, exist_ok=True)

        base = os.path.splitext(os.path.basename(self.input_path))[0]
        site = base.replace("reviews_", "")

        filename = f"preprocessed_reviews_{site}.csv"
        out_file = os.path.join(self.output_dir, filename)

        self.df.to_csv(out_file, index=False, encoding="utf-8-sig")