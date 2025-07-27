from pathlib import Path
from tclogger import OSEnver
from typing import Literal

REPO_ROOT = Path(__file__).parents[1]
CONFIGS_ROOT = REPO_ROOT / "configs"
LOGS_ROOT = REPO_ROOT / "logs"
IMGS_ROOT = REPO_ROOT / "imgs"
DATA_ROOT = REPO_ROOT / "data"

CHROME_USER_DATA_DIR = DATA_ROOT / "chrome"

SECRETS_PATH = CONFIGS_ROOT / "secrets.json"
SECRETS = OSEnver(SECRETS_PATH)
BLINKIT_LOCATIONS = SECRETS["blinkit_locations"]
SWIGGY_LOCATIONS = SECRETS["swiggy_locations"]
ZEPTO_LOCATIONS = SECRETS["zepto_locations"]
DMART_LOCATIONS = SECRETS["dmart_locations"]
LOCATION_LIST = SECRETS["location_list"]
LOCATION_MAP = SECRETS["location_map"]
BROWSER_SETTINGS = SECRETS["browser_settings"]
BLINKIT_BROWSER_SETTING = BROWSER_SETTINGS["blinkit"]
SWIGGY_BROWSER_SETTING = BROWSER_SETTINGS["swiggy"]
ZEPTO_BROWSER_SETTING = BROWSER_SETTINGS["zepto"]
DMART_BROWSER_SETTING = BROWSER_SETTINGS["dmart"]
EMAIL_SENDER = SECRETS["email_sender"]
EMAIL_RECVER = SECRETS["email_recver"]
SKU_XLSX = DATA_ROOT / SECRETS["sku_xlsx"]
HTTP_PROXY = SECRETS["http_proxy"]

WEBSITE_NAMES = ["blinkit", "zepto", "swiggy", "dmart"]
WEBSITE_LITERAL = Literal["blinkit", "zepto", "swiggy", "dmart"]
