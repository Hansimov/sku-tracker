import json
import urllib.parse

from DrissionPage._pages.chromium_tab import ChromiumTab
from pathlib import Path
from tclogger import logger, logstr, brk, get_now_str, dict_to_str, dict_get, dict_set
from time import sleep
from typing import Union

from configs.envs import DATA_ROOT, SWIGGY_LOCATIONS
from web.clicker import SwiggyLocationClicker
from web.browser import BrowserClient
from web.fetch import fetch_with_retry
from file.local_dump import LocalAddressExtractor

WEBSITE_NAME = "swiggy"
SWIGGY_MAIN_URL = "https://www.swiggy.com"
SWIGGY_ITEM_URL = "https://www.swiggy.com/stores/instamart/item"


class SwiggyLocationChecker:
    def get_correct_address(self, location_idx: int) -> str:
        return SWIGGY_LOCATIONS[location_idx].get("text", "")

    def unify_address(self, address: str) -> str:
        if address:
            return "".join(address.replace(",", "").split()[:2]).lower()
        else:
            return ""

    def check_address(
        self,
        local_address: str,
        correct_address: str,
        extra_msg: str = "",
        raise_error: bool = True,
    ):
        if not local_address:
            return False
        local_address_str = self.unify_address(local_address)
        correct_address_str = self.unify_address(correct_address)
        if local_address_str != correct_address_str:
            err_mesg = f"\n  × {extra_msg}: incorrect location!"
            logger.warn(err_mesg)
            info_dict = {
                "local_address": local_address,
                "correct_address": correct_address,
            }
            logger.mesg(dict_to_str(info_dict), indent=4)
            if raise_error:
                raise ValueError(err_mesg)
            return False
        return True

    def check_tab_location(
        self, tab: ChromiumTab, location_idx: int, extra_msg: str = ""
    ):
        cookies = tab.cookies(all_info=True).as_dict()
        user_location_raw = dict_get(cookies, "userLocation", None)
        if not user_location_raw:
            return False
        try:
            user_location_dict = json.loads(urllib.parse.unquote(user_location_raw))
            tab_address = dict_get(user_location_dict, "address", "")
            correct_address = self.get_correct_address(location_idx)
            return self.check_address(
                tab_address, correct_address, extra_msg, raise_error=False
            )
        except Exception as e:
            logger.warn(e)
            return False

    def check_product_location(
        self, product_info: dict, location_idx: int, extra_msg: str = ""
    ):
        product_address = dict_get(product_info, ["userLocation", "address"], "")
        correct_address = self.get_correct_address(location_idx)
        return self.check_address(product_address, correct_address, extra_msg)


class SwiggyLocationSwitcher:
    def __init__(self):
        self.checker = SwiggyLocationChecker()
        self.client = BrowserClient()

    def create_clicker(self):
        self.clicker = SwiggyLocationClicker()

    def set_location(self, location_idx: int = 0) -> dict:
        logger.note(f"> Visiting main page: {logstr.mesg(brk(SWIGGY_MAIN_URL))}")
        self.client.start_client()
        self.create_clicker()
        tab = self.client.browser.latest_tab
        tab.set.load_mode.none()

        tab.get(SWIGGY_MAIN_URL)
        logger.mesg(f"  ✓ Title: {brk(tab.title)}")

        if self.checker.check_tab_location(
            tab, location_idx, extra_msg="SwiggyLocationSwitcher"
        ):
            logger.okay("  * Location already correctly set. Skip.")
        else:
            logger.note(f"> Setting location:")
            location_dict = SWIGGY_LOCATIONS[location_idx]
            location_name = location_dict.get("name", "")
            location_text = location_dict.get("text", "")
            location_shot = location_dict.get("shot", "")
            logger.file(f"  * {location_name} ({location_text})")

            sleep(3)
            self.clicker.set_location_image_name("swiggy_loc_main.png")
            self.clicker.type_target_location_text(location_text)

            sleep(3)
            self.clicker.set_location_image_name(location_shot)
            self.clicker.click_target_position()

            sleep(10)

        # self.client.close_other_tabs(create_new_tab=True)
        self.client.stop_client(close_browser=False)


class SwiggyBrowserScraper:
    def __init__(self, date_str: str = None):
        self.date_str = date_str
        self.client = BrowserClient()
        self.init_paths()

    def init_paths(self):
        self.date_str = self.date_str or get_now_str()[:10]
        self.dump_root = DATA_ROOT / "dumps" / self.date_str / WEBSITE_NAME

    def get_cookies(self, tab: ChromiumTab) -> dict:
        cookies_dict = tab.cookies(all_info=True).as_dict()
        cookies_dict["url"] = tab.url
        cookies_dict["now"] = get_now_str()
        return cookies_dict

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

        self.client.start_client()

        tab = self.client.browser.latest_tab
        tab.set.load_mode.none()

        tab.get(item_url, interval=4)
        logger.mesg(f"  ✓ Title: {brk(tab.title)}")

        product_info = tab.run_js("return window.___INITIAL_STATE___;")
        if product_info and save_cookies:
            product_info = self.clean_resp(product_info)
            product_info["cookies"] = self.get_cookies(tab)

        self.client.stop_client(close_browser=False)
        return product_info

    def get_dump_path(self, product_id: Union[str, int], parent: str = None) -> Path:
        filename = f"{product_id}.json"
        if parent:
            dump_path = self.dump_root / parent / filename
        else:
            dump_path = self.dump_root / filename
        return dump_path

    def dump(self, product_id: Union[str, int], resp: dict, parent: str = None):
        logger.note(f"  > Dump product data to json:", end=" ")
        dump_path = self.get_dump_path(product_id, parent)
        dump_path.parent.mkdir(parents=True, exist_ok=True)
        with open(dump_path, "w", encoding="utf-8") as wf:
            json.dump(resp, wf, indent=4, ensure_ascii=False)
        logger.okay(f"{brk(dump_path)}")

    def run(
        self, product_id: Union[str, int], save_cookies: bool = True, parent: str = None
    ) -> dict:
        product_info = fetch_with_retry(
            self.fetch, product_id=product_id, save_cookies=save_cookies
        )
        self.dump(product_id=product_id, resp=product_info, parent=parent)
        return product_info


class SwiggyProductDataExtractor:
    def __init__(self, verbose: bool = False):
        self.verbose = verbose
        self.address_extractor = LocalAddressExtractor(website_name=WEBSITE_NAME)

    def extract_varirant(self, resp: dict, var_idx: int = 0) -> dict:
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
        variant = dict_get(item_state, ["variations", var_idx], {})

        # get product_id, product_name
        product_id = dict_get(item_state, "product_id", None)
        product_name = dict_get(variant, "display_name", None)

        # get in_stock flag (Y/N/-)
        in_stock = dict_get(item_state, "in_stock", None)
        if in_stock is True:
            in_stock_flag = 1
        elif in_stock is False:
            in_stock_flag = 0
        else:
            in_stock_flag = "N/A"

        # get price, mrp, unit
        price_dict = dict_get(variant, "price", {})
        price = dict_get(price_dict, "offer_price", None)
        mrp = dict_get(price_dict, "mrp", None)
        unit = dict_get(variant, "quantity", None)

        # get location
        location = self.address_extractor.get_column_location(resp)

        product_data = {
            "product_name": product_name,
            "product_id": product_id,
            "unit": unit,
            "price": price,
            "mrp": mrp,
            "in_stock": in_stock_flag,
            "var_idx": var_idx,
            "location": location,
        }
        logger.okay(dict_to_str(product_data), indent=4)
        logger.exit_quiet(not self.verbose)

        return product_data

    def is_price_close(
        self,
        price: Union[float, int],
        ref_price: Union[float, int],
        max_diff_ratio: float = 0.5,
    ) -> bool:
        ratio = abs(price - ref_price) / min(price, ref_price)
        return ratio < max_diff_ratio

    def check_by_ref(self, res: dict, ref_mrp: Union[int, float] = None) -> bool:
        mrp = res.get("mrp", None)
        if not self.is_price_close(mrp, ref_mrp):
            product_id = res.get("product_id", "")
            diff = abs(mrp - ref_mrp) / min(mrp, ref_mrp)
            logger.warn(
                f"\n  × Outlier variant [{product_id}]: "
                f"mrp ({mrp}), ref_mrp ({ref_mrp}), diff ({diff:.2f})\n"
            )

    def extract_closet_variant(self, resp: dict, ref_mrp: Union[int, float]) -> dict:
        variants = dict_get(
            resp, "instamart.cachedProductItemData.lastItemState.variations", []
        )
        res = {}
        variant_num = len(variants)
        for var_idx in range(variant_num):
            variant_data = self.extract_varirant(resp, var_idx=var_idx)
            variant_mrp = variant_data.get("mrp", None)
            if var_idx == 0:
                mrp_diff = abs(variant_mrp - ref_mrp)
                res = variant_data
                continue
            if variant_mrp is not None:
                diff = abs(variant_mrp - ref_mrp)
                if diff < mrp_diff:
                    mrp_diff = diff
                    res = variant_data
        if res:
            self.check_by_ref(res, ref_mrp=ref_mrp)
        else:
            url = dict_get(resp, "cookies.url", "")
            logger.warn(f"\n  × No variant: {url}", verbose=self.verbose)
        return res

    def extract(self, resp: dict, ref_mrp: Union[int, float] = None) -> list[dict]:
        """If `ref_mrp` is not None, would choose variant whose `mrp` is closest to `ref_mrp`."""
        if ref_mrp is None or ref_mrp <= 0:
            return self.extract_varirant(resp, var_idx=0)
        else:
            return self.extract_closet_variant(resp, ref_mrp=ref_mrp)


def test_browser_scraper():
    switcher = SwiggyLocationSwitcher()
    switcher.set_location(location_idx=1)

    sleep(2)

    scraper = SwiggyBrowserScraper()
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
