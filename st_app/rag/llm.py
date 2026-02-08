"""
RAG LLM wrapper: centralizes all LLM API calls for the RAG pipeline.
Other modules (e.g. rag_review_node) should import generate_text from here
instead of calling an LLM SDK directly.
"""
import os

from dotenv import load_dotenv
from langchain_upstage import ChatUpstage

# Configurable model name (can be overridden by env var)
DEFAULT_MODEL = os.getenv("LLM_MODEL", "solar-mini") 
UPSTAGE_API_KEY_ENV = "UPSTAGE_API_KEY"


def generate_text(
    system_prompt: str,
    user_prompt: str,
    temperature: float = 0.2,
) -> str:
    """
    Send system and user prompts to the LLM and return the generated text.

    Args:
        system_prompt: System/instruction prompt for the model.
        user_prompt: User message content.
        temperature: Sampling temperature (default 0.2 for more deterministic output).

    Returns:
        The generated text as a string.

    Raises:
        ValueError: If UPSTAGE_API_KEY is not set in the environment.
    """
    load_dotenv()

    api_key = os.getenv(UPSTAGE_API_KEY_ENV)
    if not api_key or not api_key.strip():
        raise ValueError(
            f"Missing API key: set {UPSTAGE_API_KEY_ENV} in your environment or .env file."
        )

    # ChatUpstage reads UPSTAGE_API_KEY from environment by default
    llm = ChatUpstage(model=DEFAULT_MODEL, temperature=temperature)

    resp = llm.invoke(
        [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_prompt},
        ]
    )

    # LangChain message objects usually have `.content`
    return (getattr(resp, "content", "") or "").strip()


if __name__ == "__main__":
    # Minimal test call (optional).
    load_dotenv()
    if not os.getenv(UPSTAGE_API_KEY_ENV):
        print(f"Skip: set {UPSTAGE_API_KEY_ENV} to run a test call.")
    else:
        out = generate_text(
            system_prompt="You reply in one short sentence.",
            user_prompt="Say hello.",
            temperature=0.0,
        )
        print("Generated:", out)

