from tclogger import logger
from time import sleep


def fetch_with_retry(
    fetch: callable,
    *args,
    max_retries: int = 3,
    retry_interval: float = 3,
    **kwargs,
):
    retry_count = 0
    res = None
    while retry_count < max_retries:
        try:
            res = fetch(*args, **kwargs)
            if res:
                break
            else:
                logger.warn(f"  × Empty response")
        except Exception as e:
            logger.warn(f"  × Fetch failed: {e}")

        retry_count += 1
        if retry_count < max_retries:
            logger.note(f"  > Retry ({retry_count}/{max_retries})")
            sleep(retry_interval)
        else:
            logger.warn(f"  × Exceed max retries ({max_retries}). Fetch aborted.")
            raise e
    return res
