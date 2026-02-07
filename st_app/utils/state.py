from typing import Annotated, TypedDict
import operator


class ChatState(TypedDict):
    messages: Annotated[list[dict], operator.add]
    route: str
    retrieved_docs: list[dict]
