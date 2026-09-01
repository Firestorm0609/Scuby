"""Ollama LLM client — wraps the local Ollama API."""

import httpx
import logging
import json
from typing import Optional

logger = logging.getLogger(__name__)


class LLMClient:
    """Client for Ollama local LLM."""

    def __init__(self, base_url: str = "http://localhost:11434",
                 model: str = "llama3", temperature: float = 0.3,
                 max_tokens: int = 500, timeout: int = 30):
        self.base_url = base_url.rstrip("/")
        self.model = model
        self.temperature = temperature
        self.max_tokens = max_tokens
        self.client = httpx.AsyncClient(timeout=timeout)

    async def chat(self, prompt: str, system: str = "") -> Optional[str]:
        """Send a prompt to the LLM and get a response.
        
        Args:
            prompt: The user message
            system: Optional system prompt
            
        Returns:
            The model's response text, or None on error
        """
        messages = []
        if system:
            messages.append({"role": "system", "content": system})
        messages.append({"role": "user", "content": prompt})

        try:
            resp = await self.client.post(
                f"{self.base_url}/api/chat",
                json={
                    "model": self.model,
                    "messages": messages,
                    "stream": False,
                    "options": {
                        "temperature": self.temperature,
                        "num_predict": self.max_tokens,
                    },
                },
            )
            resp.raise_for_status()
            data = resp.json()
            
            content = data.get("message", {}).get("content", "")
            logger.debug(f"LLM response: {content[:200]}...")
            return content

        except httpx.ConnectError:
            logger.error(
                f"Cannot connect to Ollama at {self.base_url}. "
                "Is Ollama running? Start with: ollama serve"
            )
            return None
        except Exception as e:
            logger.error(f"LLM request failed: {e}")
            return None

    async def chat_json(self, prompt: str, system: str = "") -> Optional[dict]:
        """Send a prompt and parse the response as JSON.
        
        Tries to extract JSON from the response, handling cases where
        the LLM wraps it in markdown code blocks.
        """
        response = await self.chat(prompt, system)
        if response is None:
            return None

        # Try direct JSON parse
        try:
            return json.loads(response)
        except json.JSONDecodeError:
            pass

        # Try extracting from code blocks
        import re
        json_match = re.search(r'```(?:json)?\s*\n?(.*?)\n?```', response, re.DOTALL)
        if json_match:
            try:
                return json.loads(json_match.group(1))
            except json.JSONDecodeError:
                pass

        # Try finding any JSON object in the text
        json_match = re.search(r'\{[^{}]*\}', response, re.DOTALL)
        if json_match:
            try:
                return json.loads(json_match.group(0))
            except json.JSONDecodeError:
                pass

        logger.warning(f"Failed to parse JSON from LLM response: {response[:200]}")
        return None

    async def health_check(self) -> bool:
        """Check if Ollama is running and the model is available."""
        try:
            resp = await self.client.get(f"{self.base_url}/api/tags")
            resp.raise_for_status()
            models = resp.json().get("models", [])
            model_names = [m.get("name", "") for m in models]
            
            if any(self.model in name for name in model_names):
                logger.info(f"Ollama healthy. Model '{self.model}' available.")
                return True
            else:
                logger.warning(
                    f"Model '{self.model}' not found. Available: {model_names}. "
                    f"Run: ollama pull {self.model}"
                )
                return False
        except Exception as e:
            logger.error(f"Ollama health check failed: {e}")
            return False

    async def close(self):
        await self.client.aclose()
