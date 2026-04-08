import logging
from typing import Callable, Any
from tenacity import (
    retry,
    stop_after_attempt,
    wait_exponential,
    retry_if_exception_type,
    before_sleep_log,
)

logger = logging.getLogger("nachomarket.resilience")


def retry_with_backoff(
    max_attempts: int = 3,
    min_wait: float = 1.0,
    max_wait: float = 30.0,
    exceptions: tuple[type[Exception], ...] = (Exception,),
) -> Callable:
    """Decorator para retry con exponential backoff."""
    return retry(
        stop=stop_after_attempt(max_attempts),
        wait=wait_exponential(multiplier=1, min=min_wait, max=max_wait),
        retry=retry_if_exception_type(exceptions),
        before_sleep=before_sleep_log(logger, logging.WARNING),
        reraise=True,
    )


class HealthChecker:
    """Monitorea la salud de las conexiones del bot."""

    def __init__(self) -> None:
        self._checks: dict[str, Callable[[], bool]] = {}

    def register(self, name: str, check_fn: Callable[[], bool]) -> None:
        """Registra un health check."""
        self._checks[name] = check_fn

    def run_all(self) -> dict[str, bool]:
        """Ejecuta todos los health checks."""
        results: dict[str, bool] = {}
        for name, check_fn in self._checks.items():
            try:
                results[name] = check_fn()
            except Exception:
                logger.exception(f"Health check failed: {name}")
                results[name] = False
        return results

    def is_healthy(self) -> bool:
        """Retorna True si todos los checks pasan."""
        return all(self.run_all().values())
