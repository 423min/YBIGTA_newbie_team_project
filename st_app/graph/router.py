import os
from langchain_upstage import ChatUpstage

ROUTING_PROMPT = """You are a routing classifier for a Zootopia movie chatbot. Given the user's message, output exactly one   
  label.                                                                                                                        
                                                                                                                                
  Labels:                                                                                                                       
  - chat: greetings, small talk, off-topic, general questions unrelated to the movie Zootopia
  - subject: questions about Zootopia's factual information (director, cast, voice actors, characters, plot, release year,
  genre, awards, box office, sequel)
  - review: questions about audience opinions, what people thought, ratings, sentiment, public reception, how the movie was
  received, or comparisons of viewer reactions

  Examples:
  - "Hello!" → chat
  - "Who directed Zootopia?" → subject
  - "Who voices Nick Wilde?" → subject
  - "What do audiences say about the themes?" → review
  - "Was Zootopia good?" → review
  - "Tell me a joke" → chat

  User message: {message}

  Respond with ONLY one word: chat, subject, or review."""


def route(state: dict) -> str:
    messages = state.get("messages", [])

    last_user_msg = ""
    for msg in reversed(messages):
        if msg.get("role") == "user":
            last_user_msg = msg.get("content", "")
            break

    if not last_user_msg:
        return "chat"

    llm = ChatUpstage(
        api_key=os.environ.get("UPSTAGE_API_KEY"),
        model="solar-mini",
    )

    response = llm.invoke(ROUTING_PROMPT.format(message=last_user_msg))
    label = response.content.strip().lower().strip("\"'.")

    if label not in ("chat", "subject", "review"):
        return "chat"

    return label
