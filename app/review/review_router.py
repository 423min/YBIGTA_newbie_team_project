from __future__ import annotations

from datetime import datetime, timezone
from fastapi import APIRouter, Depends, HTTPException
import pandas as pd

from app.dependencies import get_mongo_db


from review_analysis.preprocessing.imdb_processor import IMDbProcessor
from review_analysis.preprocessing.letterboxd_processor import LetterboxdProcessor
from review_analysis.preprocessing.rottentomatoes_processor import RottenTomatoesProcessor

router = APIRouter(prefix="/review", tags=["review"])

PROCESSOR_MAP = {
    "imdb": IMDbProcessor,
    "letterboxd": LetterboxdProcessor,
    "rottentomatoes": RottenTomatoesProcessor,
}

COLLECTION_MAP = {
        "imdb": "REVIEW_imdb",
        "letterboxd": "REVIEW_letterboxd",
        "rottentomatoes": "REVIEW_rottentomatoes", 
}

@router.post("/preprocess/{site_name}")
def preprocess_site(site_name: str, db=Depends(get_mongo_db)):
    site_name = site_name.lower().strip()
    if site_name not in PROCESSOR_MAP:
        raise HTTPException(
            status_code=400,
            detail="site_name must be one of: imdb, letterboxd, rottentomatoes",
        )

    raw_col = db[COLLECTION_MAP[site_name]]
    pre_col = db[f"PREPROCESSED_{COLLECTION_MAP[site_name]}"]
   
    print("DEBUG DB:", db.name)
    print("DEBUG RAW COL:", raw_col.name, "COUNT:", raw_col.count_documents({}))

    docs = list(raw_col.find({"preprocessed": {"$ne": True}}))

    if not docs:
        return {
            "status": "success",
            "site": site_name,
            "raw_fetched": 0,
            "processed_inserted": 0,
            "message": "No new documents to preprocess.",
        }

    
    df_raw = pd.DataFrame(docs)
    source_ids = df_raw["_id"].tolist()
    df_for_proc = df_raw.drop(columns=["_id"], errors="ignore")

    
    processor_cls = PROCESSOR_MAP[site_name]
    processor = processor_cls(input_path="", output_path="") 
    processor.preprocess(df=df_for_proc)
    processor.feature_engineering()

    df_out = processor.df
    if df_out is None or df_out.empty:
        return {
            "status": "success",
            "site": site_name,
            "raw_fetched": len(docs),
            "processed_inserted": 0,
            "message": "Preprocess finished but produced empty result.",
        }

   
    df_out = df_out.copy()

   
    if "year_month" in df_out.columns:
        df_out["year_month"] = df_out["year_month"].astype(str)

  
    if "date" in df_out.columns:
        df_out["date"] = pd.to_datetime(df_out["date"], errors="coerce").dt.to_pydatetime()

  
    df_out["source_id"] = source_ids[: len(df_out)]
    df_out["site"] = site_name
    df_out["created_at"] = datetime.now(timezone.utc)

    records = df_out.to_dict("records")


    result = pre_col.insert_many(records)


    raw_col.update_many(
        {"_id": {"$in": source_ids}},
        {"$set": {"preprocessed": True, "preprocessed_at": datetime.now(timezone.utc)}},
    )

    return {
        "status": "success",
        "site": site_name,
        "raw_fetched": len(docs),
        "processed_inserted": len(result.inserted_ids),
    }
