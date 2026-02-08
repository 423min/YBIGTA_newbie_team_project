"""
RAG embedder: load review CSVs, generate embeddings, and save FAISS index.
"""
import json
from pathlib import Path

from dotenv import load_dotenv
from langchain_community.document_loaders import CSVLoader, TextLoader
from langchain_community.vectorstores import FAISS
from langchain_text_splitters import RecursiveCharacterTextSplitter
from langchain_upstage import UpstageEmbeddings


def create_vector_db() -> None:
    """Load review data, build embeddings, and save FAISS index to st_app/db/faiss_index."""
    load_dotenv()

    # Paths: script lives in st_app/rag/, project root is two levels up
    project_root = Path(__file__).resolve().parent.parent.parent
    index_dir = Path(__file__).resolve().parent.parent / "db" / "faiss_index"

    input_files = [
        project_root / "database" / "preprocessed_reviews_imdb.csv",
        project_root / "database" / "preprocessed_reviews_letterboxd.csv",
        project_root / "database" / "preprocessed_reviews_RottenTomatoes.csv",
    ]

    print("Loading data...")
    all_docs = []
    for path in input_files:
        if not path.exists():
            raise FileNotFoundError(f"Input file not found: {path}")
        if path.suffix == ".csv":
            loader = CSVLoader(str(path), encoding="utf-8")
        else:
            loader = TextLoader(str(path), encoding="utf-8")
        all_docs.extend(loader.load())

    print(f"Loaded {len(all_docs)} document(s). Splitting into chunks...")
    splitter = RecursiveCharacterTextSplitter(
        chunk_size=500,
        chunk_overlap=50,
    )
    chunks = splitter.split_documents(all_docs)
    print(f"Created {len(chunks)} chunks.")

    print("Creating index...")
    embeddings = UpstageEmbeddings(model="solar-embedding-1-large")
    vectorstore = FAISS.from_documents(chunks, embeddings)

    index_dir.mkdir(parents=True, exist_ok=True)
    vectorstore.save_local(str(index_dir))
    print("Saved successfully to", index_dir)

    print("Generating meta.json...")
    
    meta_data = []
    for i, chunk in enumerate(chunks):
        meta_data.append({
            "id": i,
            "content": chunk.page_content,
            "metadata": chunk.metadata
        })

    meta_path = index_dir / "meta.json"
    with open(meta_path, "w", encoding="utf-8") as f:
        json.dump(meta_data, f, ensure_ascii=False, indent=4)
    
    print(f"Saved meta.json to {meta_path}")


if __name__ == "__main__":
    create_vector_db()
