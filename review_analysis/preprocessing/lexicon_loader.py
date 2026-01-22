import pandas as pd # type: ignore
import os

def load_lexicon(lexicon_path: str = "review_analysis/preprocessing/movie_review_lexicon_400.csv") -> dict[str, float]:
    """
    주관성 분석을 위한 감정 어휘 사전을 로드하고 단어별 가중치 딕셔너리를 반환합니다.
    
    가중치 기준:
        - strongsubj (강한 주관성): 2.0
        - verystrongsubj (매우 강한 주관성): 4.0
        
    Args:
        lexicon_path (str): 감정 어휘 사전 CSV 파일 경로.
        
    Returns:
        dict[str, float]: 단어를 키로, 주관성 가중치를 값으로 하는 딕셔너리.
    """
    if not os.path.exists(lexicon_path):
        # 실행 위치에 따른 상대 경로 폴백 설정
        base_dir = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
        lexicon_path = os.path.join(base_dir, "review_analysis", "preprocessing", "movie_review_lexicon_400.csv")
    
    try:
        df = pd.read_csv(lexicon_path)
    except FileNotFoundError:
        print(f"경고: {lexicon_path}에서 어휘 사전 파일을 찾을 수 없습니다. 빈 사전을 반환합니다.")
        return {}
    
    # 관련 타입 필터링 및 가중치 매핑
    weight_map = {
        "strongsubj": 2.0,
        "verystrongsubj": 4.0
    }
    
    lexicon_dict = {}
    
    for _, row in df.iterrows():
        word = str(row['word']).strip().lower()
        subj_type = str(row['type']).strip().lower()
        
        if subj_type in weight_map:
            lexicon_dict[word] = weight_map[subj_type]
            
    return lexicon_dict
