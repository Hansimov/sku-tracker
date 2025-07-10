from pathlib import Path
from tclogger import OSEnver

REPO_ROOT = Path(__file__).parents[1]
CONFIGS_ROOT = REPO_ROOT / "configs"
LOGS_ROOT = REPO_ROOT / "logs"
IMGS_ROOT = REPO_ROOT / "imgs"

SECRETS_PATH = CONFIGS_ROOT / "secrets.json"
SECRETS = OSEnver(SECRETS_PATH)
LOCATIONS = SECRETS["locations"]
