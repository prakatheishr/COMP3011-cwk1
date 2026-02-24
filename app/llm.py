import requests


OLLAMA_URL = "http://localhost:11434/api/generate"
MODEL_NAME = "llama3"


def generate_llm_insight(facts: dict, mode: str, fmt: str) -> str:
    """
    Sends structured fact pack to local LLM and asks for narrative output.
    """

    system_prompt = (
        "You are a professional Formula 1 analyst. "
        "You will receive structured JSON data about a race or season. "
        "Use ONLY the provided data. Do not invent information."
    )

    style_instruction = (
        "Write in fan-style radio commentary."
        if fmt == "radio"
        else "Write in concise analytical report style."
    )

    intent_instruction = (
        "Focus on what happened."
        if mode == "recap"
        else "Focus on why it mattered for the championship or competitive balance."
    )

    prompt = f"""
{system_prompt}

FACTS:
{facts}

INSTRUCTIONS:
{intent_instruction}
{style_instruction}
Keep response under 200 words.
"""

    response = requests.post(
        OLLAMA_URL,
        json={
            "model": MODEL_NAME,
            "prompt": prompt,
            "stream": False,
        },
        timeout=60,
    )

    if response.status_code != 200:
        return "LLM generation failed."

    data = response.json()
    return data.get("response", "").strip()