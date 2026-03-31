import json
import os
from typing import Dict, List, Tuple
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen


class AIProviderError(Exception):
    def __init__(self, provider: str, kind: str, message: str):
        super().__init__(message)
        self.provider = provider
        self.kind = kind
        self.message = message


class AIService:
    def __init__(self):
        self.enabled = os.getenv("AI_ENABLED", "false").lower() == "true"
        self.provider = os.getenv("AI_PROVIDER", "auto").lower()
        self.timeout_sec = int(os.getenv("AI_TIMEOUT_SEC", "10"))

        self.groq_key = os.getenv("GROQ_API_KEY", "")
        self.groq_model = os.getenv("AI_MODEL", "llama-3.1-8b-instant")
        self.groq_url = "https://api.groq.com/openai/v1/chat/completions"

        self.openrouter_key = os.getenv("OPENROUTER_API_KEY", "")
        self.openrouter_model = os.getenv(
            "OPENROUTER_MODEL",
            "openrouter/auto",
        )
        self.openrouter_url = os.getenv(
            "OPENROUTER_BASE_URL",
            "https://openrouter.ai/api/v1/chat/completions",
        )
        self.last_provider_used = "-"
        self.last_model_used = "-"

    def is_available(self) -> Tuple[bool, str]:
        if not self.enabled:
            return False, "ИИ отключен. Установите AI_ENABLED=true"

        if self.provider == "groq":
            if not self.groq_key:
                return False, "Не задан GROQ_API_KEY"
            return True, "ok"

        if self.provider == "openrouter":
            if not self.openrouter_key:
                return False, "Не задан OPENROUTER_API_KEY"
            return True, "ok"

        if self.provider == "auto":
            if self.groq_key or self.openrouter_key:
                return True, "ok"
            return False, "Для AI_PROVIDER=auto нужен GROQ_API_KEY или OPENROUTER_API_KEY"

        return False, "AI_PROVIDER должен быть: groq, openrouter или auto"

    def ask(self, system_prompt: str, user_prompt: str, max_tokens: int = 500) -> str:
        self.last_provider_used = "-"
        self.last_model_used = "-"
        available, reason = self.is_available()
        if not available:
            raise RuntimeError(reason)

        providers = self._resolve_provider_order()
        errors: List[AIProviderError] = []

        for provider in providers:
            try:
                if provider == "groq":
                    return self._ask_groq(system_prompt, user_prompt, max_tokens)
                if provider == "openrouter":
                    return self._ask_openrouter(system_prompt, user_prompt, max_tokens)
            except AIProviderError as e:
                errors.append(e)
                if not self._should_fallback(e):
                    break

        if errors:
            details = " | ".join([f"{e.provider}:{e.kind}:{e.message}" for e in errors])
            raise RuntimeError(f"AI providers failed: {details}")

        raise RuntimeError("AI providers failed: no valid provider configured")

    def _resolve_provider_order(self) -> List[str]:
        if self.provider == "groq":
            return ["groq"]
        if self.provider == "openrouter":
            return ["openrouter"]

        order: List[str] = []
        if self.groq_key:
            order.append("groq")
        if self.openrouter_key:
            order.append("openrouter")
        return order

    def _should_fallback(self, err: AIProviderError) -> bool:
        return err.kind in {"blocked", "timeout", "network", "server", "rate_limit"}

    def _ask_groq(self, system_prompt: str, user_prompt: str, max_tokens: int) -> str:
        if not self.groq_key:
            raise AIProviderError("groq", "config", "GROQ_API_KEY is missing")

        return self._post_chat(
            provider="groq",
            url=self.groq_url,
            api_key=self.groq_key,
            model=self.groq_model,
            system_prompt=system_prompt,
            user_prompt=user_prompt,
            max_tokens=max_tokens,
            extra_headers={},
        )

    def _ask_openrouter(self, system_prompt: str, user_prompt: str, max_tokens: int) -> str:
        if not self.openrouter_key:
            raise AIProviderError("openrouter", "config", "OPENROUTER_API_KEY is missing")

        try:
            return self._post_chat(
                provider="openrouter",
                url=self.openrouter_url,
                api_key=self.openrouter_key,
                model=self.openrouter_model,
                system_prompt=system_prompt,
                user_prompt=user_prompt,
                max_tokens=max_tokens,
                extra_headers={
                    "HTTP-Referer": "https://railway.app",
                    "X-Title": "fc-mobile-bot",
                },
            )
        except AIProviderError as e:
            if "No endpoints found" in e.message and self.openrouter_model != "openrouter/auto":
                return self._post_chat(
                    provider="openrouter",
                    url=self.openrouter_url,
                    api_key=self.openrouter_key,
                    model="openrouter/auto",
                    system_prompt=system_prompt,
                    user_prompt=user_prompt,
                    max_tokens=max_tokens,
                    extra_headers={
                        "HTTP-Referer": "https://railway.app",
                        "X-Title": "fc-mobile-bot",
                    },
                )
            raise

    def _post_chat(
        self,
        provider: str,
        url: str,
        api_key: str,
        model: str,
        system_prompt: str,
        user_prompt: str,
        max_tokens: int,
        extra_headers: Dict[str, str],
    ) -> str:
        payload = {
            "model": model,
            "messages": [
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt},
            ],
            "temperature": 0.3,
            "max_tokens": max_tokens,
        }

        headers = {
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json",
        }
        headers.update(extra_headers)

        req = Request(
            url,
            data=json.dumps(payload).encode("utf-8"),
            headers=headers,
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
            lower = body.lower()

            if e.code in (401, 403):
                kind = "auth"
                if "1010" in lower or "access denied" in lower or "cloudflare" in lower:
                    kind = "blocked"
                raise AIProviderError(provider, kind, f"HTTP {e.code}: {body[:250]}")
            if e.code == 429:
                raise AIProviderError(provider, "rate_limit", f"HTTP 429: {body[:250]}")
            if 500 <= e.code <= 599:
                raise AIProviderError(provider, "server", f"HTTP {e.code}: {body[:250]}")
            raise AIProviderError(provider, "http", f"HTTP {e.code}: {body[:250]}")
        except URLError as e:
            raise AIProviderError(provider, "network", str(e))
        except TimeoutError as e:
            raise AIProviderError(provider, "timeout", str(e))
        except Exception as e:
            raise AIProviderError(provider, "request", str(e))

        try:
            content = response_data["choices"][0]["message"]["content"].strip()
            if not content:
                raise ValueError("empty content")
            self.last_provider_used = provider
            self.last_model_used = model
            return content
        except Exception:
            raise AIProviderError(provider, "format", "Unexpected provider response format")
