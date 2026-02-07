import os
from langchain_upstage import ChatUpstage
from langchain_core.messages import SystemMessage, HumanMessage, AIMessage

SYSTEM_PROMPT = "You are a friendly and helpful chatbot. Respond naturally and conversationally. If the user asks a specific question about Zootopia's movie details (cast, plot, director) or audience reviews/opinions, let them know you can help with that too — just ask directly."


def chat_node(state: dict) -> dict:
    llm = ChatUpstage(
        api_key=os.environ.get("UPSTAGE_API_KEY"),
        model="solar-mini",
    )

    lc_messages = [SystemMessage(content=SYSTEM_PROMPT)]
    for msg in state.get("messages", []):
        role = msg.get("role")
        content = msg.get("content", "")
        if role == "user":
            lc_messages.append(HumanMessage(content=content))
        elif role == "assistant":
            lc_messages.append(AIMessage(content=content))

    response = llm.invoke(lc_messages)

    return {
        "messages": [{"role": "assistant", "content": response.content}],
        "route": "chat",
    }
