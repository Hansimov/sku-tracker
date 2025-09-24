import ast
import json
import re

from DrissionPage._pages.chromium_tab import ChromiumTab
from time import sleep
from typing import Union
from tclogger import logger, logstr, brk, get_now_str, dict_set_all

from configs.envs import DATA_ROOT, BLINKIT_LOCATIONS, BLINKIT_TRAVERSER_SETTING
from web.blinkit.scraper import BlinkitLocationChecker, BlinkitLocationSwitcher
from web.browser import BrowserClient
from web.fetch import fetch_with_retry

WEBSITE_NAME = "blinkit"
BLINKIT_CATEG_URL = "https://blinkit.com/categories"
BLINKIT_FLAG_URL = "https://blinkit.com/api/feature-flags/receive"
BLINKIT_CATEG_JS = "https://blinkit.com/.*/categories.*js"
BLINKIT_DEEPLINK_URL = "https://blinkit.com/v2/search/deeplink"
# -/(\.(js|woff|css|svg|ico|png)|data:)/


class BlinkitCategoriesExtractor:
    def __init__(self, client: BrowserClient, verbose: bool = False):
        self.client = client
        self.verbose = verbose

    def js_to_json_str(self, js_str: str) -> str:
        """Convert JS object notation to JSON string"""
        tab = self.client.browser.latest_tab
        # JSON.stringify()
        json_str = tab.run_js(
            f"""JSON.stringify({js_str});""",
            as_expr=True,
        )
        return json_str

    def extract_categories_from_js_str(self, js_str: str) -> list:
        """Extract a.CATEGORY data from JavaScript content"""
        if not js_str:
            logger.warn("  × Empty js str")
            return []
        pattern = r"a\.CATEGORY\s*=\s*(\[.*?\])},\s*66557"
        match = re.search(pattern, js_str, re.DOTALL)
        if not match:
            logger.warn("  × Not find `a.CATEGORY` pattern")
            return []

        categories = []
        match_str = match.group(1)
        logger.okay(f"  + Found `a.CATEGORY` pattern in js")
        try:
            json_str = self.js_to_json_str(match_str)
            categories = json.loads(json_str)
            logger.okay(f"  + Parsed {len(categories)} categories")
        except Exception as e:
            logger.warn(f"  × Failed to parse categories from js: {e}")
            return []

        return categories

    def extract(self, js_str: str) -> dict:
        logger.enter_quiet(not self.verbose)
        if not js_str:
            logger.warn("  × Empty response js to extract")
            logger.exit_quiet(not self.verbose)
            return {}
        categ_data = {}
        categories = self.extract_categories_from_js_str(js_str)
        if categories:
            categ_data = {"categories": categories, "count": len(categories)}
        else:
            categ_data = {"categories": [], "count": -1}
        logger.exit_quiet(not self.verbose)
        return categ_data


class BlinkitCategoriesFetcher:
    def __init__(self, client: BrowserClient, date_str: str = None):
        self.client = client
        self.date_str = date_str
        self.extractor = BlinkitCategoriesExtractor(client=client, verbose=True)
        self.init_paths()

    def init_paths(self):
        self.date_str = self.date_str or get_now_str()[:10]
        self.dump_root = DATA_ROOT / "traverses" / self.date_str / WEBSITE_NAME
        self.dump_path = self.dump_root / "categories.json"

    def get_cookies(self, tab: ChromiumTab) -> dict:
        cookies_dict = tab.cookies(all_info=True).as_dict()
        cookies_dict["url"] = tab.url
        cookies_dict["now"] = get_now_str()
        return cookies_dict

    def fetch(self) -> dict:
        categ_url = BLINKIT_CATEG_URL
        logger.note(f"> Visiting categories url:")
        logger.file(f"  * {categ_url}")

        self.client.start_client()
        tab = self.client.browser.latest_tab
        tab.set.load_mode.none()

        listen_targets = [BLINKIT_FLAG_URL, BLINKIT_CATEG_JS]
        tab.listen.start(targets=listen_targets, is_regex=True)
        tab.get(categ_url)
        logger.mesg(f"  ✓ Title: {brk(tab.title)}")

        logger.note(f"  > Listening targets:")
        for target in listen_targets:
            logger.file(f"    * {target}")

        categ_packet = None
        categ_data = {}
        for packet in tab.listen.steps(timeout=30):
            packet_url = packet.url
            packet_url_str = logstr.file(brk(packet_url))
            if packet_url == BLINKIT_FLAG_URL:
                logger.okay(f"  + Flags packet captured: {packet_url_str}")
            elif re.match(BLINKIT_CATEG_JS, packet_url):
                logger.okay(f"  + Categories JS packet captured: {packet_url_str}")
                tab.stop_loading()
                categ_packet = packet
                break
            else:
                logger.warn(f"  × Unexpected packet: {packet_url_str}")

        if categ_packet:
            categ_resp = categ_packet.response
            if categ_resp:
                categ_js_str = categ_resp.body
                categ_data = self.extractor.extract(categ_js_str)

        if categ_data:
            categ_data["cookies"] = self.get_cookies(tab)

        self.client.stop_client(close_browser=False)
        return categ_data

    def dump(self, resp: dict):
        logger.note(f"  > Dump categories data to json:", end=" ")
        self.dump_path.parent.mkdir(parents=True, exist_ok=True)
        with open(self.dump_path, "w", encoding="utf-8") as wf:
            json.dump(resp, wf, indent=4, ensure_ascii=False)
        logger.okay(f"{brk(self.dump_path)}")

    def run(self):
        resp_data = self.fetch()
        if resp_data:
            if resp_data:
                self.dump(resp_data)
        else:
            return {}


class BlinkitProductsTraverser:
    def __init__(
        self,
        skip_exists: bool = True,
        date_str: str = None,
        client_settings: dict = None,
        locations: list = None,
    ):
        self.skip_exists = skip_exists
        self.date_str = date_str or get_now_str()[:10]
        self.client_settings = client_settings or BLINKIT_TRAVERSER_SETTING
        self.locations = locations or BLINKIT_LOCATIONS
        self.client = BrowserClient(**self.client_settings)
        self.checker = BlinkitLocationChecker(locations=self.locations)
        self.switcher = BlinkitLocationSwitcher(
            client_settings=self.client_settings, locations=self.locations
        )
        self.fetcher = BlinkitCategoriesFetcher(
            client=self.client, date_str=self.date_str
        )

    def run(self):
        for location_idx, location_item in enumerate(self.locations[:1]):
            location_name = location_item.get("name", "")
            location_text = location_item.get("text", "")
            is_set_location = False
            if not is_set_location:
                logger.hint(f"> New Location: {location_name} ({location_text})")
                self.switcher.set_location(location_idx)
                is_set_location = True
            self.fetcher.run()


def test_traverser():
    traverser = BlinkitProductsTraverser(skip_exists=False, date_str=None)
    traverser.run()


if __name__ == "__main__":
    test_traverser()

    # python -m web.blinkit.traverser
