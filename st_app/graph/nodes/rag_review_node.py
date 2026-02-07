import os
from langchain_upstage import ChatUpstage
from langchain_core.messages import SystemMessage, HumanMessage
from st_app.rag.retriever import retrieve

SYSTEM_PROMPT = """You are a movie review analyst. Answer the user's question based on the following audience review snippets. Synthesize the reviews into a coherent answer. If the reviews don't address the question, say so.

Retrieved reviews:
{reviews}"""


def rag_review_node(state: dict) -> dict:
    llm = ChatUpstage(
        api_key=os.environ.get("UPSTAGE_API_KEY"),
        model="solar-mini",
    )

    last_user_msg = ""
    for msg in reversed(state.get("messages", [])):
        if msg.get("role") == "user":
            last_user_msg = msg.get("content", "")
            break

    docs = retrieve(last_user_msg, k=5)

    reviews_text = "\n".join(
        f"- [{d.get('source', 'unknown')}] {d.get('text', '')}" for d in docs
    )

    lc_messages = [
        SystemMessage(content=SYSTEM_PROMPT.format(reviews=reviews_text)),
        HumanMessage(content=last_user_msg),
    ]

    response = llm.invoke(lc_messages)

    return {
        "messages": [{"role": "assistant", "content": response.content}],
        "route": "review",
        "retrieved_docs": docs,
    }
