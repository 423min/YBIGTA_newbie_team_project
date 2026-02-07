import json
import os
from pathlib import Path
from langchain_upstage import ChatUpstage
from langchain_core.messages import SystemMessage, HumanMessage

SUBJECTS_PATH = (
    Path(__file__).resolve().parent.parent.parent
    / "db"
    / "subject_information"
    / "subjects.json"
)

SYSTEM_PROMPT = """You are a movie information assistant. Answer the user's question using ONLY the provided movie data below. Be concise and accurate. If the data does not contain the answer, say so.

Movie data:
{subject_data}"""


def _load_subjects() -> str:
    with open(SUBJECTS_PATH, "r", encoding="utf-8") as f:
        data = json.load(f)
    return json.dumps(data, ensure_ascii=False, indent=2)


def subject_info_node(state: dict) -> dict:
    llm = ChatUpstage(
        api_key=os.environ.get("UPSTAGE_API_KEY"),
        model="solar-mini",
    )

    subject_data = _load_subjects()

    last_user_msg = ""
    for msg in reversed(state.get("messages", [])):
        if msg.get("role") == "user":
            last_user_msg = msg.get("content", "")
            break

    lc_messages = [
        SystemMessage(content=SYSTEM_PROMPT.format(subject_data=subject_data)),
        HumanMessage(content=last_user_msg),
    ]

    response = llm.invoke(lc_messages)

    return {
        "messages": [{"role": "assistant", "content": response.content}],
        "route": "subject",
    }
