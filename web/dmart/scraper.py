import json
import urllib.parse

from bs4 import BeautifulSoup
from DrissionPage._pages.chromium_tab import ChromiumTab
from pathlib import Path
from tclogger import logger, logstr, brk, get_now_str, dict_to_str
from tclogger import dict_get, dict_set, dict_set_all
from time import sleep
from typing import Union

from configs.envs import DATA_ROOT, DMART_LOCATIONS, DMART_BROWSER_SETTING
from web.browser import BrowserClient
from web.fetch import fetch_with_retry
from file.local_dump import LocalAddressExtractor

WEBSITE_NAME = "dmart"
DMART_MAIN_URL = "https://www.dmart.in"
DMART_ITEM_URL = "https://www.dmart.in/product"


def deserialize_str_to_json(json_str: str) -> dict:
    """Deserialize JSON-style string to Python dictionary."""
    json_str = bytes(json_str, "utf-8").decode("unicode_escape")
    return json.loads(json_str)


def url_to_filename(url: str) -> str:
    return urllib.parse.quote(url, safe="")


def filename_to_url(filename: str) -> str:
    return urllib.parse.unquote(filename)


class DmartLocationChecker:
    def get_correct_address(self, location_idx: int) -> str:
        return DMART_LOCATIONS[location_idx].get("locality", "")

    def check_address(
        self,
        local_address: str,
        correct_address: str,
        extra_msg: str = "",
        raise_error: bool = True,
    ):
        if not local_address:
            return False
        if correct_address.lower() not in local_address.lower():
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
        guest_info = dict_get(cookies, "guest", None)
        if not guest_info:
            return False
        try:
            guest_info_str = urllib.parse.unquote(guest_info)
            guest_info_dict = deserialize_str_to_json(guest_info_str)
            tab_address = dict_get(guest_info_dict, "preferredPIN", "")
            correct_address = self.get_correct_address(location_idx)
            return self.check_address(
                tab_address, correct_address, extra_msg, raise_error=False
            )
        except Exception as e:
            logger.warn(e)
            logger.mesg(guest_info)
            return False

    def check_product_location(
        self, product_info: dict, location_idx: int, extra_msg: str = ""
    ):
        product_address = dict_get(product_info, "cookies.guest.preferredPIN", "")
        correct_address = self.get_correct_address(location_idx)
        return self.check_address(product_address, correct_address, extra_msg)


class DmartLocationSwitcher:
    def __init__(self):
        self.checker = DmartLocationChecker()
        self.client = BrowserClient(**DMART_BROWSER_SETTING)

    def set_location(self, location_idx: int = 0) -> dict:
        logger.note(f"> Visiting main page: {logstr.mesg(brk(DMART_MAIN_URL))}")
        self.client.start_client()
        tab = self.client.browser.latest_tab
        tab.set.load_mode.none()

        tab.get(DMART_MAIN_URL, timeout=30)
        logger.mesg(f"  ✓ Title: {brk(tab.title)}")

        if self.checker.check_tab_location(
            tab, location_idx, extra_msg="DmartLocationSwitcher"
        ):
            logger.okay("  * Location already correctly set. Skip.")
        else:
            logger.note(f"> Setting location:")
            location_dict = DMART_LOCATIONS[location_idx]
            location_name = location_dict.get("name", "")
            location_text = location_dict.get("text", "")
            logger.file(f"  * {location_name} ({location_text})")

            sleep(3)
            location_button = tab.ele(".^header_pincode")
            logger.mesg(f"  * Click location button ...")
            location_button.click()

            sleep(1)
            location_input = tab.ele("#pincodeInput")
            sleep(1)
            logger.mesg(f"  * Input target location text ...")
            location_input.input(location_text)

            sleep(2)
            location_item = tab.ele(".^pincode-widget_pincode-item")
            logger.mesg(f"  * Click most-related location suggestion ...")
            location_item.click()

            sleep(2)
            confirm_region = tab.ele(".^pincode-widget_success-cntr-footer")
            confirm_button = confirm_region.ele("xpath=//button")
            logger.mesg(f"  * Click confirm button ...")
            confirm_button.click()

            sleep(3)

        # self.client.close_other_tabs(create_new_tab=True)
        self.client.stop_client(close_browser=False)


class DmartResponseParser:
    def extract_resp(self, html: str) -> dict:
        soup = BeautifulSoup(html, "html.parser")
        target_ele = soup.find("script", id="__NEXT_DATA__", type="application/json")
        resp = json.loads(target_ele.string.strip())
        return resp

    def clean_resp(self, resp: dict) -> dict:
        res = dict_get(resp, "props.pageProps", {})
        dict_set(res, "pdpData.dynamicPDP.data.widgets", [])
        dict_set(res, "pdpData.dynamicPDP.data.customizeAttributes", {})
        dict_set_all(res, "descriptionTabs", [], ignore_case=True, use_regex=True)
        return res


class DmartBrowserScraper:
    def __init__(self, date_str: str = None):
        self.date_str = date_str
        self.client = BrowserClient(**DMART_BROWSER_SETTING)
        self.init_paths()
        self.init_resp_parser()

    def init_paths(self):
        self.date_str = self.date_str or get_now_str()[:10]
        self.dump_root = DATA_ROOT / "dumps" / self.date_str / WEBSITE_NAME

    def init_resp_parser(self):
        self.resp_parser = DmartResponseParser()

    def get_cookies(self, tab: ChromiumTab) -> dict:
        cookies_dict = tab.cookies(all_info=True).as_dict()
        for k, v in cookies_dict.items():
            try:
                cookies_dict[k] = urllib.parse.unquote(v)
                cookies_dict[k] = deserialize_str_to_json(cookies_dict[k])
            except Exception as e:
                pass
        cookies_dict["url"] = tab.url
        cookies_dict["now"] = get_now_str()
        return cookies_dict

    def fetch(self, product_id: Union[str, int], save_cookies: bool = True) -> dict:
        item_url = f"{DMART_ITEM_URL}/{product_id}"
        logger.note(f"> Visiting product page: {logstr.mesg(brk(product_id))}")
        logger.file(f"  * {item_url}")

        self.client.start_client()
        tab = self.client.browser.latest_tab
        tab.set.load_mode.none()

        tab.get(item_url, interval=4)
        logger.mesg(f"  ✓ Title: {brk(tab.title)}")

        product_info = {}
        resp = self.resp_parser.extract_resp(tab.html)
        if resp and save_cookies:
            resp = self.resp_parser.clean_resp(resp)
            product_info = {"resp": resp}
            product_info["cookies"] = self.get_cookies(tab)
            product_info["product_id"] = url_to_filename(product_id)

        self.client.stop_client(close_browser=False)
        return product_info

    def get_dump_path(self, product_id: Union[str, int], parent: str = None) -> Path:
        filename = f"{url_to_filename(str(product_id))}.json"
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
            self.fetch, product_id=product_id, save_cookies=save_cookies, max_retries=5
        )
        self.dump(product_id=product_id, resp=product_info, parent=parent)
        return product_info


class DmartProductDataExtractor:
    def __init__(self, verbose: bool = False):
        self.verbose = verbose
        self.addr_extractor = LocalAddressExtractor(website_name=WEBSITE_NAME)

    def extract_variant(self, info: dict, var_idx: int = 0) -> dict:
        logger.enter_quiet(not self.verbose)
        if not info:
            logger.warn("  × Empty response data to extract")
            logger.exit_quiet(not self.verbose)
            return {}

        logger.note(f"  > Extracting product Data ...")

        # get product_id
        product_id = dict_get(info, "product_id", "")

        # get location
        location = self.addr_extractor.get_column_location(info)

        # get skus
        skus = dict_get(info, "resp.pdpData.dynamicPDP.data.productData.sKUs")
        if not skus:
            logger.warn("  × No skus found in response data")
            logger.exit_quiet(not self.verbose)
            return {
                "product_id": product_id,
                # in_stock="" means page is "Currently Unavailable".
                # From the perspective of software engineering,
                # it should not be set to "" as it would cause ambiguity,
                # but the customer requires so.
                "in_stock": "",
                "location": location,
            }

        # get target sku by index
        sku = skus[var_idx] if var_idx < len(skus) else skus[0]

        # get product_name
        product_name = dict_get(sku, "name", "")

        # get in_stock flag
        inv_type = dict_get(sku, "invType", "")
        if inv_type.lower() == "a":
            in_stock_flag = 1
        else:
            in_stock_flag = 0

        # get price, mrp, unit
        price = dict_get(sku, "priceSALE", None)
        if price:
            price = int(float(price))

        mrp = dict_get(sku, "priceMRP", None)
        if mrp:
            mrp = int(float(mrp))

        unit = dict_get(sku, "variantTextValue", "")

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
                f"mrp ({mrp}), ref_mrp ({ref_mrp}), diff ({diff:.2f})"
            )
            return False
        return True

    def extract_closest_variant(self, info: dict, ref_mrp: Union[int, float]) -> dict:
        skus = dict_get(info, "resp.pdpData.dynamicPDP.data.productData.sKUs", [])
        res = {}
        variant_num = len(skus)
        for var_idx in range(variant_num):
            variant_data = self.extract_variant(info, var_idx=var_idx)
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
            ref_check_res = self.check_by_ref(res, ref_mrp=ref_mrp)
            if not ref_check_res:
                res["in_stock"] = 0
        else:
            url = dict_get(info, "cookies.url", "")
            logger.warn(f"\n  × No variant: {url}", verbose=self.verbose)
        return res

    def extract(self, info: dict, ref_mrp: Union[int, float] = None) -> dict:
        """If `ref_mrp` is not None, would choose variant whose `mrp` is closest to `ref_mrp`."""
        if ref_mrp is None or ref_mrp <= 0:
            res = self.extract_variant(info, var_idx=0)
        else:
            res = self.extract_closest_variant(info, ref_mrp=ref_mrp)
        return res


def test_browser_scraper():
    switcher = DmartLocationSwitcher()
    switcher.set_location(location_idx=0)

    scraper = DmartBrowserScraper()
    product_id = "fortune-chakki-fresh-atta-patta0fort45xx160320?selectedProd=713128"
    product_info = scraper.fetch(product_id, save_cookies=True)
    scraper.dump(product_id, product_info)

    extractor = DmartProductDataExtractor(verbose=True)
    extractor.extract(product_info)


if __name__ == "__main__":
    test_browser_scraper()

    # python -m web.dmart.scraper
