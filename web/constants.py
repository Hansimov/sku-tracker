from tclogger import get_now_str


def norm_date_str(date_str: str = None) -> str:
    return date_str or get_now_str()[:10]
