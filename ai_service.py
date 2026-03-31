import json
import os
from typing import Tuple
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen


class AIService:
    def __init__(self):
        self.enabled = os.getenv("AI_ENABLED", "false").lower() == "true"
        self.provider = os.getenv("AI_PROVIDER", "").lower()
        self.api_key = os.getenv("GROQ_API_KEY", "")
        self.model = os.getenv("AI_MODEL", "llama-3.1-8b-instant")
        self.timeout_sec = int(os.getenv("AI_TIMEOUT_SEC", "10"))
        self.base_url = "https://api.groq.com/openai/v1/chat/completions"

    def is_available(self) -> Tuple[bool, str]:
        if not self.enabled:
            return False, "ИИ отключен. Установите AI_ENABLED=true"
        if self.provider != "groq":
            return False, "Поддерживается только AI_PROVIDER=groq"
        if not self.api_key:
            return False, "Не задан GROQ_API_KEY"
        return True, "ok"

    def ask(self, system_prompt: str, user_prompt: str, max_tokens: int = 500) -> str:
        available, reason = self.is_available()
        if not available:
            raise RuntimeError(reason)

        payload = {
            "model": self.model,
            "messages": [
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt},
            ],
            "temperature": 0.3,
            "max_tokens": max_tokens,
        }

        req = Request(
            self.base_url,
            data=json.dumps(payload).encode("utf-8"),
            headers={
                "Authorization": f"Bearer {self.api_key}",
                "Content-Type": "application/json",
            },
            method="POST",
        )

        try:
            with urlopen(req, timeout=self.timeout_sec) as resp:
                response_data = json.loads(resp.read().decode("utf-8"))
        except HTTPError as e:
            body = ""
            try:
                body = e.read().decode("utf-8", errors="ignore")
            except Exception:
                pass
            raise RuntimeError(f"Groq HTTP {e.code}: {body[:300]}")
        except URLError as e:
            raise RuntimeError(f"Сетевая ошибка: {e}")
        except Exception as e:
            raise RuntimeError(f"Ошибка AI запроса: {e}")

        try:
            return response_data["choices"][0]["message"]["content"].strip()
        except Exception:
            raise RuntimeError("Неожиданный формат ответа Groq")
