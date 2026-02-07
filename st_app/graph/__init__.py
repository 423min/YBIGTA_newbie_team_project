from langgraph.graph import StateGraph, START, END
from st_app.utils.state import ChatState
from st_app.graph.router import route
from st_app.graph.nodes.chat_node import chat_node
from st_app.graph.nodes.subject_info_node import subject_info_node
from st_app.graph.nodes.rag_review_node import rag_review_node

graph = StateGraph(ChatState)

graph.add_node("chat", chat_node)
graph.add_node("subject", subject_info_node)
graph.add_node("rag_review", rag_review_node)

graph.add_conditional_edges(
    START,
    route,
    {"chat": "chat", "subject": "subject", "review": "rag_review"},
)

graph.add_edge("chat", END)
graph.add_edge("subject", END)
graph.add_edge("rag_review", END)

app = graph.compile()
