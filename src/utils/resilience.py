import logging
from typing import Callable, Any
from requests import HTTPError
from tenacity import (
    retry,
    stop_after_attempt,
    wait_exponential,
    retry_if_exception_type,
    retry_if_exception,
    before_sleep_log,
)

logger = logging.getLogger("nachomarket.resilience")


def _is_permanent_error(exc: Exception) -> bool:
    """Retorna True si el error es permanente y no vale la pena reintentar."""
    # Errores de programacion/cliente — no se arreglan con retry
    if isinstance(exc, (AttributeError, TypeError, ValueError)):
        return True

    # Requests HTTPError
    if isinstance(exc, HTTPError):
        if 400 <= exc.response.status_code < 500 and exc.response.status_code != 429:
            return True
    
    # PolyApiException oficial de py-clob-client-v2
    if hasattr(exc, 'status_code'):
        status = getattr(exc, 'status_code')
        if status in (404, 405, 400, 401, 403):
            return True
    
    # Buscar status code dentro del mensaje de error
    err_str = str(exc).lower()
    if 'status_code=404' in err_str or '404 client error' in err_str:
        return True
    if 'status_code=405' in err_str or '405 client error' in err_str:
        return True
        
    return False


def _should_retry(exc: Exception) -> bool:
    """Retorna True si el error amerita retry."""
    return not _is_permanent_error(exc)


def retry_with_backoff(
    max_attempts: int = 3,
    min_wait: float = 1.0,
    max_wait: float = 30.0,
    exceptions: tuple[type[Exception], ...] = (Exception,),
) -> Callable:
    """Decorator para retry con exponential backoff.
    No reintenta errores permanentes 404, 405, etc.
    """
    return retry(
        stop=stop_after_attempt(max_attempts),
        wait=wait_exponential(multiplier=1, min=min_wait, max=max_wait),
        retry=retry_if_exception_type(exceptions) & retry_if_exception(_should_retry),
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
