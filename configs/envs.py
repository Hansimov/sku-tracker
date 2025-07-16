from pathlib import Path
from tclogger import OSEnver

REPO_ROOT = Path(__file__).parents[1]
CONFIGS_ROOT = REPO_ROOT / "configs"
LOGS_ROOT = REPO_ROOT / "logs"
IMGS_ROOT = REPO_ROOT / "imgs"
DATA_ROOT = REPO_ROOT / "data"

SECRETS_PATH = CONFIGS_ROOT / "secrets.json"
SECRETS = OSEnver(SECRETS_PATH)
BLINKIT_LOCATIONS = SECRETS["blinkit_locations"]
SWIGGY_LOCATIONS = SECRETS["swiggy_locations"]
ZEPTO_LOCATIONS = SECRETS["zepto_locations"]
LOCATION_LIST = SECRETS["location_list"]
LOCATION_MAP = SECRETS["location_map"]
SKU_XLSX = DATA_ROOT / SECRETS["sku_xlsx"]
HTTP_PROXY = SECRETS["http_proxy"]
