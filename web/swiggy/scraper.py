import json

from DrissionPage import Chromium, ChromiumOptions
from DrissionPage._pages.chromium_tab import ChromiumTab
from pathlib import Path
from pyvirtualdisplay import Display
from tclogger import logger, logstr, brk, get_now_str, dict_to_str, dict_get, dict_set
from time import sleep
from typing import Union

from configs.envs import DATA_ROOT, SWIGGY_LOCATIONS
from web.clicker import SwiggyLocationClicker

SWIGGY_MAIN_URL = "https://www.swiggy.com"
SWIGGY_ITEM_URL = "https://www.swiggy.com/stores/instamart/item"


class SwiggyLocationSwitcher:
    def __init__(self, use_virtual_display: bool = False):
        self.use_virtual_display = use_virtual_display
        self.init_virtual_display()
        self.init_browser()
        self.init_location_clicker()

    def init_virtual_display(self):
        self.is_using_virtual_display = False
        if self.use_virtual_display:
            self.display = Display()
            self.start_virtual_display()

    def init_browser(self):
        chrome_options = ChromiumOptions()
        self.browser = Chromium(addr_or_opts=chrome_options)
        self.chrome_options = chrome_options

    def init_location_clicker(self):
        self.location_clicker = SwiggyLocationClicker()

    def start_virtual_display(self):
        self.display.start()
        self.is_using_virtual_display = True

    def stop_virtual_display(self):
        if self.is_using_virtual_display:
            self.display.stop()
            self.is_using_virtual_display = False

    def set_location(self, location_idx: int = 0) -> dict:
        logger.note(f"> Visiting main page: {logstr.mesg(brk(SWIGGY_MAIN_URL))}")
        tab = self.browser.latest_tab
        tab.set.load_mode.none()

        tab.get(SWIGGY_MAIN_URL)
        logger.mesg(f"  ✓ Title: {brk(tab.title)}")

        logger.note(f"> Setting location:")
        location_dict = SWIGGY_LOCATIONS[location_idx]
        location_name = location_dict.get("name", "")
        location_text = location_dict.get("text", "")
        location_shot = location_dict.get("shot", "")
        logger.file(f"  * {location_name} ({location_text})")

        sleep(3)
        self.location_clicker.set_location_image_name("swiggy_loc_main.png")
        self.location_clicker.type_target_location_text(location_text)

        sleep(3)
        self.location_clicker.set_location_image_name(location_shot)
        self.location_clicker.click_target_location()

        sleep(3)

        self.stop_virtual_display()


class SwiggyBrowserScraper:
    def __init__(self, use_virtual_display: bool = False):
        self.use_virtual_display = use_virtual_display
        self.init_virtual_display()
        self.init_browser()
        self.init_paths()

    def init_virtual_display(self):
        self.is_using_virtual_display = False
        if self.use_virtual_display:
            self.display = Display()
            self.start_virtual_display()

    def init_browser(self):
        chrome_options = ChromiumOptions()
        self.browser = Chromium(addr_or_opts=chrome_options)
        self.chrome_options = chrome_options

    def init_paths(self):
        date_str = get_now_str()[:10]
        self.dump_root = DATA_ROOT / "dumps" / date_str / "swiggy"

    def start_virtual_display(self):
        self.display.start()
        self.is_using_virtual_display = True

    def stop_virtual_display(self):
        if self.is_using_virtual_display:
            self.display.stop()
            self.is_using_virtual_display = False

    def get_cookies(self, tab: ChromiumTab) -> dict:
        cookies_dict = tab.cookies(all_info=True).as_dict()
        cookies_dict["url"] = tab.url
        cookies_dict["now"] = get_now_str()
        return cookies_dict

    def new_tab(self) -> ChromiumTab:
        return self.browser.new_tab()

    def clean_resp(self, resp: dict) -> dict:
        dict_set(resp, "storeDetailsV2", {})
        dict_set(resp, "misc", {})
        dict_set(resp, ["instamart", "footerData"], {})
        dict_set(resp, ["instamart", "cachedProductItemData", "widgetsState"], [])
        return resp

    def fetch(self, product_id: Union[str, int], save_cookies: bool = True) -> dict:
        item_url = f"{SWIGGY_ITEM_URL}/{product_id}"
        logger.note(f"> Visiting product page: {logstr.mesg(brk(product_id))}")
        logger.file(f"  * {item_url}")

        tab = self.browser.latest_tab
        tab.set.load_mode.none()

        tab.get(item_url, interval=4)
        logger.mesg(f"  ✓ Title: {brk(tab.title)}")

        product_info = tab.run_js("return window.___INITIAL_STATE___;")
        if product_info and save_cookies:
            product_info = self.clean_resp(product_info)
            product_info["cookies"] = self.get_cookies(tab)

        self.stop_virtual_display()
        return product_info

    def fetch_with_retry(
        self,
        product_id: Union[str, int],
        save_cookies: bool = True,
        max_retries: int = 3,
    ):
        retry_count = 0
        res = None
        while retry_count < max_retries:
            try:
                res = self.fetch(product_id=product_id, save_cookies=save_cookies)
                if res:
                    break
            except Exception as e:
                logger.warn(f"  × Fetch failed: {e}")

            retry_count += 1
            if retry_count < max_retries:
                logger.note(f"  > Retry ({retry_count}/{max_retries})")
                sleep(3)
            else:
                err_mesg = f"  × Exceed max retries ({max_retries}), aborted"
                logger.warn(err_mesg)
                raise RuntimeError(err_mesg)

        return res

    def get_dump_path(self, product_id: Union[str, int], parent: str = None) -> Path:
        filename = f"{product_id}.json"
        if parent:
            dump_path = self.dump_root / parent / filename
        else:
            dump_path = self.dump_root / filename
        return dump_path

    def dump(self, product_id: Union[str, int], resp: dict, parent: str = None):
        logger.note(f"  > Dumping product data to json ...")
        dump_path = self.get_dump_path(product_id, parent)
        dump_path.parent.mkdir(parents=True, exist_ok=True)
        with open(dump_path, "w", encoding="utf-8") as wf:
            json.dump(resp, wf, indent=4, ensure_ascii=False)
        logger.okay(f"    * {dump_path}")

    def run(
        self, product_id: Union[str, int], save_cookies: bool = True, parent: str = None
    ) -> dict:
        product_info = self.fetch_with_retry(
            product_id=product_id, save_cookies=save_cookies
        )
        self.dump(product_id=product_id, resp=product_info, parent=parent)
        return product_info


class SwiggyProductDataExtractor:
    def __init__(self, verbose: bool = False):
        self.verbose = verbose

    def extract(self, resp: dict) -> dict:
        logger.enter_quiet(not self.verbose)
        if not resp:
            logger.warn("  × Empty response data to extract")
            logger.exit_quiet(not self.verbose)
            return {}

        logger.note(f"  > Extracting product Data ...")

        # meta info
        item_state = dict_get(
            resp, ["instamart", "cachedProductItemData", "lastItemState"], {}
        )
        var0 = dict_get(item_state, ["variations", 0], {})

        # get product_id, product_name
        product_id = dict_get(item_state, "product_id", None)
        product_name = dict_get(var0, "display_name", None)

        # get in_stock flag (Y/N/-)
        in_stock = dict_get(item_state, "in_stock", None)
        if in_stock is True:
            in_stock_flag = "Y"
        elif in_stock is False:
            in_stock_flag = "N"
        else:
            in_stock_flag = "-"

        # get price, mrp, unit
        price_dict = dict_get(var0, "price", {})
        price = dict_get(price_dict, "offer_price", None)
        mrp = dict_get(price_dict, "mrp", None)
        unit = dict_get(var0, "quantity", None)

        product_data = {
            "product_name": product_name,
            "product_id": product_id,
            "unit": unit,
            "price": price,
            "mrp": mrp,
            "in_stock": in_stock_flag,
        }
        logger.okay(dict_to_str(product_data), indent=4)
        logger.exit_quiet(not self.verbose)

        return product_data


def test_browser_scraper():
    switcher = SwiggyLocationSwitcher(use_virtual_display=False)
    switcher.set_location(location_idx=2)

    scraper = SwiggyBrowserScraper(use_virtual_display=False)
    scraper.new_tab()
    # product_id = "MW5MP8UE57"
    # product_id = "A05X4XH0BU"
    product_id = "YR2XETQJK3"
    product_info = scraper.fetch(product_id, save_cookies=True)
    scraper.dump(product_id, product_info)

    extractor = SwiggyProductDataExtractor(verbose=True)
    extractor.extract(product_info)


if __name__ == "__main__":
    test_browser_scraper()

    # python -m web.swiggy.scraper
