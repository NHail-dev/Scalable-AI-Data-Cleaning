import time
import logging

logger = logging.getLogger(__name__)

def benchmark(func):
    """Decorator to benchmark function execution time."""
    def wrapper(*args, **kwargs):
        start = time.perf_counter()
        result = func(*args, **kwargs)
        end = time.perf_counter()
        logger.info(f"Function '{func.__name__}' completed in {end - start:.2f} seconds")
        return result
    return wrapper

