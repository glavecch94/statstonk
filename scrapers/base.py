from __future__ import annotations

import logging
import random
import time
from typing import Any
from urllib.parse import urlparse

import requests
from requests import Response, Session
from tenacity import (
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
)

from config import RATE_LIMITS

logger = logging.getLogger(__name__)


class RateLimiter:
    """
    Rate limiter per dominio. Tiene traccia dell'ultimo request e aggiunge
    un'attesa randomizzata per evitare pattern troppo regolari.
    """

    def __init__(self) -> None:
        self._last_request: dict[str, float] = {}

    def wait(self, domain: str) -> None:
        delay = RATE_LIMITS.get(domain, RATE_LIMITS["default"])
        last = self._last_request.get(domain, 0.0)
        elapsed = time.time() - last

        if elapsed < delay:
            # jitter: ±0.5-1.5s sopra il delay minimo
            sleep_time = delay - elapsed + random.uniform(0.5, 1.5)
            logger.debug(f"Rate limit [{domain}]: attendo {sleep_time:.1f}s")
            time.sleep(sleep_time)

        self._last_request[domain] = time.time()


# Singleton condiviso tra tutti gli scraper per rispettare i rate limit globali
_rate_limiter = RateLimiter()


class BaseScraper:
    """
    Classe base per tutti gli scraper.

    Ogni scraper figlio deve definire:
        BASE_URL: str      — dominio base della fonte
        SOURCE_NAME: str   — etichetta leggibile (es. "fbref", "understat")
    """

    BASE_URL: str = ""
    SOURCE_NAME: str = ""

    def __init__(self) -> None:
        self.session = Session()
        self.session.headers.update(
            {
                "User-Agent": (
                    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/120.0.0.0 Safari/537.36"
                ),
                "Accept-Language": "it-IT,it;q=0.9,en-US;q=0.8,en;q=0.7",
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            }
        )

    @property
    def _domain(self) -> str:
        return urlparse(self.BASE_URL).netloc

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=5, max=30),
        retry=retry_if_exception_type((requests.ConnectionError, requests.Timeout)),
        reraise=True,
    )
    def get(self, url: str, **kwargs: Any) -> Response:
        domain = urlparse(url).netloc or self._domain
        _rate_limiter.wait(domain)
        logger.info(f"[{self.SOURCE_NAME}] GET {url}")
        response = self.session.get(url, timeout=30, **kwargs)
        response.raise_for_status()
        return response

    def get_json(self, url: str, **kwargs: Any) -> Any:
        return self.get(url, **kwargs).json()

    def close(self) -> None:
        self.session.close()

    def __enter__(self) -> BaseScraper:
        return self

    def __exit__(self, *args: Any) -> None:
        self.close()
