"""
RAG retriever: load FAISS index and return top-k similar documents for a query.
"""
from pathlib import Path

from dotenv import load_dotenv
from langchain_community.vectorstores import FAISS
from langchain_core.documents import Document
from langchain_upstage import UpstageEmbeddings

# Project-root-based path: script lives in st_app/rag/, index is st_app/db/faiss_index
FAISS_INDEX_DIR = Path(__file__).resolve().parent.parent / "db" / "faiss_index"


def retrieve(query: str, top_k: int = 3) -> list[Document]:
    """
    Load the FAISS index and return the top_k most similar documents for the query.

    Args:
        query: User query string.
        top_k: Number of documents to return (default 3).

    Returns:
        List of Document objects (each has .page_content and .metadata).
    """
    if not FAISS_INDEX_DIR.exists():
        print(f"Error: FAISS index path does not exist: {FAISS_INDEX_DIR}")
        print("Run the embedder first to create the index (e.g. python -m st_app.rag.embedder).")
        raise FileNotFoundError(f"FAISS index not found: {FAISS_INDEX_DIR}")

    load_dotenv()
    embeddings = UpstageEmbeddings(model="solar-embedding-1-large")
    vectorstore = FAISS.load_local(
        str(FAISS_INDEX_DIR),
        embeddings,
        allow_dangerous_deserialization=True,
    )
    docs = vectorstore.similarity_search(query, k=top_k)
    return docs


def retrieve_debug(query: str, top_k: int = 3) -> list[dict]:
    """
    Same as retrieve() but returns a debug-friendly list of dicts with 'content' and 'metadata'.
    """
    docs = retrieve(query, top_k=top_k)
    return [{"content": d.page_content, "metadata": d.metadata} for d in docs]


if __name__ == "__main__":
    import sys

    q = sys.argv[1] if len(sys.argv) > 1 else "great movie with good acting"
    k = int(sys.argv[2]) if len(sys.argv) > 2 else 3
    print(f"Query: {q!r}\ntop_k: {k}\n")
    try:
        results = retrieve(q, top_k=k)
        for i, doc in enumerate(results, 1):
            print(f"--- Result {i} ---")
            print(doc.page_content[:200] + ("..." if len(doc.page_content) > 200 else ""))
            print("Metadata:", doc.metadata)
            print()
    except FileNotFoundError:
        sys.exit(1)
