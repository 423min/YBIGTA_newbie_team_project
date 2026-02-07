# 임시
from typing import List, Dict

def retrieve(query: str, k: int = 5) -> List[Dict]:
    # TODO: RAG 담당자가 FAISS 기반으로 교체
    return [
        {"id": "stub-1", "source": "stub", "rating": None,
         "text": "STUB REVIEW: Many viewers loved the buddy-cop chemistry between Judy and Nick."},
        {"id": "stub-2", "source": "stub", "rating": None,
         "text": "STUB REVIEW: The film's message about bias and stereotypes resonated with audiences."},
    ][:k]