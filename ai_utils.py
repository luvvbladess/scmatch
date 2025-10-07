from typing import Any, Dict, List, Optional

import httpx
from openai import AsyncOpenAI, APIConnectionError, RateLimitError, APIStatusError

from config import CHAT_MODEL, EMBED_MODEL, OPENAI_API_KEY, OPENAI_TIMEOUT, logger

_httpx_client: Optional[httpx.AsyncClient] = httpx.AsyncClient(timeout=OPENAI_TIMEOUT)
openai_client = AsyncOpenAI(api_key=OPENAI_API_KEY, http_client=_httpx_client)

def cosine_similarity(a: List[float], b: List[float]) -> float:
    if not a or not b:
        return 0.0
    if len(a) != len(b):
        return 0.0
    dot = 0.0
    na = 0.0
    nb = 0.0
    for x, y in zip(a, b):
        dot += x * y
        na += x * x
        nb += y * y
    if na == 0 or nb == 0:
        return 0.0
    import math
    return dot / (math.sqrt(na) * math.sqrt(nb))

async def get_text_embedding(text: str) -> Optional[List[float]]:
    text = (text or "").strip()
    if not text:
        return None
    try:
        resp = await openai_client.embeddings.create(
            model=EMBED_MODEL,
            input=text,
        )
        vec = resp.data[0].embedding
        return list(vec)
    except (APIConnectionError, RateLimitError, APIStatusError) as e:
        logger.warning(f"OpenAI embedding error: {e}")
    except Exception as e:
        logger.exception(f"OpenAI embedding unexpected error: {e}")
    return None

async def virtual_reply(
    user_profile: Dict[str, Any],
    partner_gender: str,
    history: List[Dict[str, str]],
    user_message: str,
) -> str:
    hist = history[-10:] if history else []
    system_prompt = (
        f"Ты виртуальный собеседник для сервиса знакомств. Общайся дружелюбно, мило и чуть флиртующе. "
        f"Отвечай коротко (1-3 предложения). Не задавай слишком личных вопросов сразу. "
        f"Говори на русском. Ты {'мужчина' if partner_gender=='M' else 'женщина'}. "
        f"Тебе примерно {user_profile.get('age', 25)} лет. "
        f"Собеседник из {user_profile.get('city','неизвестно')}."
    )
    messages = [{"role": "system", "content": system_prompt}]
    for m in hist:
        messages.append({"role": m["role"], "content": m["content"]})
    messages.append({"role": "user", "content": user_message})
    try:
        resp = await openai_client.chat.completions.create(
            model=CHAT_MODEL,
            messages=messages,
            temperature=0.8,
            max_tokens=180,
        )
        answer = resp.choices[0].message.content.strip()
        return answer
    except (APIConnectionError, RateLimitError, APIStatusError) as e:
        logger.warning(f"OpenAI chat error: {e}")
        return "Сейчас я немного занят(а). Попробуйте написать еще раз через минутку."
    except Exception as e:
        logger.exception(f"OpenAI chat unexpected error: {e}")
        return "Упс, что-то пошло не так. Попробуйте еще раз."

async def aclose_http_client():
    global _httpx_client
    if _httpx_client is not None:
        try:
            await _httpx_client.aclose()
        finally:
            _httpx_client = None