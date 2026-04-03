import time

from starlette.requests import Request
from starlette.middleware.base import BaseHTTPMiddleware

from app import get_logger

logger = get_logger()

TIMED_PREFIXES = ("/events/",)


class TimingMiddleware(BaseHTTPMiddleware):
    """
    Middleware for timing endpoint requests. The time
    taken will be logged out
    """

    async def dispatch(self, request: Request, call_next):
        path = request.url.path

        # Only log out for intensive endpoints (events)
        if not path.startswith(TIMED_PREFIXES):
            return await call_next(request)

        start_time = time.perf_counter()

        response = await call_next(request)

        process_time = round(
            time.perf_counter() - start_time,
            4
        )

        logger.info(
            f"{request.method} {path} "
            f"took {process_time}s"
        )

        return response
