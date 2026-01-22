import pandas as pd
import os

def load_lexicon(lexicon_path: str = "review_analysis/preprocessing/movie_review_lexicon_400.csv") -> dict[str, float]:
    """
    Loads the lexicon CSV and returns a dictionary of word weights.
    
    Weights:
        - strongsubj: 2.0
        - verystrongsubj: 4.0
        
    Args:
        lexicon_path: Path to the lexicon CSV file.
        
    Returns:
        Dictionary mapping words to their subjectivity weights.
    """
    if not os.path.exists(lexicon_path):
        # Fallback for relative paths if run from different cwd
        base_dir = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
        lexicon_path = os.path.join(base_dir, "review_analysis", "preprocessing", "movie_review_lexicon_400.csv")
    
    try:
        df = pd.read_csv(lexicon_path)
    except FileNotFoundError:
        print(f"Warning: Lexicon file not found at {lexicon_path}. Returning empty lexicon.")
        return {}
    
    # Filter for relevant types and map to weights
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
