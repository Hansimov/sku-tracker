import json
import re

from DrissionPage import Chromium, ChromiumOptions
from DrissionPage._pages.chromium_tab import ChromiumTab
from pathlib import Path
from pyvirtualdisplay import Display
from tclogger import logger, logstr, brk, get_now_str, dict_to_str, dict_get, dict_set
from time import sleep
from typing import Union

from configs.envs import DATA_ROOT, ZEPTO_LOCATIONS, HTTP_PROXY

ZEPTO_MAIN_URL = "https://www.zeptonow.com"
ZEPTO_ITEM_URL = "https://www.zeptonow.com/pn/x/pvid"
ZEPTO_PAGE_URL = "https://cdn.bff.zeptonow.com/api/v2/get_page"


class ZeptoLocationSwitcher:
    def __init__(self, use_virtual_display: bool = False):
        self.use_virtual_display = use_virtual_display
        self.init_virtual_display()
        self.init_browser()

    def init_virtual_display(self):
        self.is_using_virtual_display = False
        if self.use_virtual_display:
            self.display = Display()
            self.start_virtual_display()

    def init_browser(self):
        chrome_options = ChromiumOptions()
        chrome_options.set_argument(f"--proxy-server={HTTP_PROXY}")
        self.browser = Chromium(addr_or_opts=chrome_options)
        self.chrome_options = chrome_options

    def start_virtual_display(self):
        self.display.start()
        self.is_using_virtual_display = True

    def stop_virtual_display(self):
        if self.is_using_virtual_display:
            self.display.stop()
            self.is_using_virtual_display = False

    def set_location(self, location_idx: int = 0) -> dict:
        logger.note(f"> Visiting main page: {logstr.mesg(brk(ZEPTO_MAIN_URL))}")
        tab = self.browser.latest_tab
        tab.set.load_mode.none()

        tab.get(ZEPTO_MAIN_URL)
        logger.mesg(f"  ✓ Title: {brk(tab.title)}")

        logger.note(f"> Setting location:")
        location_dict = ZEPTO_LOCATIONS[location_idx]
        location_name = location_dict.get("name", "")
        location_text = location_dict.get("text", "")
        logger.file(f"  * {location_name} ({location_text})")

        sleep(3)
        location_button = tab.ele("xpath=//button[@aria-label='Select Location']")
        location_button.click()
        sleep(1)
        location_input = tab.ele(
            "xpath=//div[@data-testid='address-search-input']//input"
        )
        sleep(1)
        location_input.input(location_text)

        sleep(2)
        location_container = tab.ele(
            "xpath=//div[@data-testid='address-search-container']//div[1]"
        )
        location_container.click()

        sleep(2)
        confirm_button = tab.ele(
            "xpath=//div[@class='map-view-with-search-map-container']//button[@data-testid='location-confirm-btn']"
        )
        confirm_button.click()

        sleep(3)

        self.stop_virtual_display()


class ZeptoResponseParser:
    def extract_resp(self, html: str) -> list:
        pattern = r'__next_f\.push\(\[1,\s*"c:(.*?)"\]\)'
        matches = re.findall(pattern, html, flags=re.DOTALL)
        results = []
        for match in matches:
            match_str = bytes(match, "utf-8").decode("unicode_escape")
            data = json.loads(match_str)
            results.append(data)
        if len(results) == 1:
            return results[0]
        else:
            return results

    def flatten_resp(self, resp: Union[list, dict]) -> Union[list, dict]:
        """
        Recursively flattens the response by removing ["$", "<tag>", null] patterns
        and uplifting single dictionary items.

        - Input: ["$","div", null, {...}] -> Output: {...}
        - If list has only one dict after filtering, uplift it to parent level
        """
        if isinstance(resp, dict):
            result = {}
            for key, value in resp.items():
                result[key] = self.flatten_resp(value)
            return result
        elif isinstance(resp, list):
            # Check if this is a ["$", "<tag>", null, {...}] pattern
            if len(resp) >= 4 and resp[0] == "$" and resp[2] is None:
                # Extract the dictionary part (index 3 onwards)
                dict_items = resp[3:]
                if len(dict_items) == 1 and isinstance(dict_items[0], dict):
                    # Single dict - uplift it and process recursively
                    return self.flatten_resp(dict_items[0])
                else:
                    # Multiple items - process each recursively
                    return [self.flatten_resp(item) for item in dict_items]
            else:
                # Regular list - filter out ["$", "<tag>", null] patterns and process remaining items
                filtered_items = []
                for item in resp:
                    if (
                        isinstance(item, list)
                        and len(item) >= 3
                        and item[0] == "$"
                        and item[2] is None
                    ):
                        # This is a ["$", "<tag>", null, {...}] pattern
                        if len(item) > 3:
                            # Extract dict parts (index 3 onwards)
                            dict_parts = item[3:]
                            for dict_part in dict_parts:
                                if isinstance(dict_part, (dict, list)):
                                    filtered_items.append(self.flatten_resp(dict_part))
                    elif isinstance(item, (dict, list)):
                        # Regular dict or list - process recursively
                        filtered_items.append(self.flatten_resp(item))
                    else:
                        # Primitive value - keep as is
                        filtered_items.append(item)

                # If only one item remains, uplift it
                if len(filtered_items) == 1:
                    return filtered_items[0]
                else:
                    return filtered_items
        else:
            # Primitive type - return as is
            return resp

    def reduce_resp(self, resp: dict) -> dict:
        res = dict_get(resp, ["children", -1], {})
        # widgets = dict_get(res, ["pageLayout", "widgets"], [])
        dict_set(res, "pageLayout.widgets", [])
        dict_set(res, "pageLayout.header.Widget", {})
        dict_set(
            res,
            "pageLayout.header.widget.data.productInfo.productVariant.l4AttributesResponse",
            {},
        )
        dict_set(
            res, "pageLayout.header.widget.data.productInfo.productVariant.images", []
        )
        dict_set(res, "pageLayout.pageData", {})
        dict_set(res, "pageLayout.pageMeta", {})
        dict_set(res, "pageLayout.header.widget.data.productInfo.storeProduct.meta", {})
        dict_set(res, "externalVendorServiceabilityInfo", {})
        return res

    def clean_resp(self, resp: list) -> dict:
        resp = self.flatten_resp(resp)
        resp = self.reduce_resp(resp)
        return resp


class ZeptoBrowserScraper:
    def __init__(self, use_virtual_display: bool = False):
        self.use_virtual_display = use_virtual_display
        self.init_virtual_display()
        self.init_browser()
        self.init_paths()
        self.init_resp_parser()

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
        self.dump_root = DATA_ROOT / "dumps" / date_str / "zepto"

    def init_resp_parser(self):
        self.resp_parser = ZeptoResponseParser()

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

    def fetch(self, product_id: Union[str, int], save_cookies: bool = True) -> dict:
        item_url = f"{ZEPTO_ITEM_URL}/{product_id}"
        logger.note(f"> Visiting product page: {logstr.mesg(brk(product_id))}")
        logger.file(f"  * {item_url}")

        tab = self.browser.latest_tab
        tab.set.load_mode.none()

        tab.get(item_url, interval=4)
        logger.mesg(f"  ✓ Title: {brk(tab.title)}")

        product_info = {}
        resp = self.resp_parser.extract_resp(tab.html)
        if resp and save_cookies:
            resp = self.resp_parser.clean_resp(resp)
            product_info = {"resp": resp}
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


class ZeptoProductDataExtractor:
    def __init__(self, verbose: bool = False):
        self.verbose = verbose

    def extract(self, info: dict) -> dict:
        logger.enter_quiet(not self.verbose)
        if not info:
            logger.warn("  × Empty response data to extract")
            logger.exit_quiet(not self.verbose)
            return {}

        logger.note(f"  > Extracting product Data ...")

        # meta info
        prd_info = dict_get(info, "resp.pageLayout.header.widget.data.productInfo", {})
        product = dict_get(prd_info, "storeProduct", {})

        # get product_id, product_name
        product_id = dict_get(info, "resp.pvid", None)
        product_name = dict_get(prd_info, "product.name", None)

        # get in_stock flag (Y/N/-)
        available_num = dict_get(product, "availableQuantity", None)
        in_stock_flag = "-"
        if isinstance(available_num, int):
            if available_num > 0:
                in_stock_flag = "Y"
            else:
                in_stock_flag = "N"

        # get price, mrp, unit
        price = dict_get(product, "discountedSellingPrice", None)
        mrp = dict_get(product, "mrp", None)
        super_price = dict_get(product, "superSaverSellingPrice", None)
        unit = dict_get(prd_info, "productVariant.formattedPacksize", None)

        product_data = {
            "product_name": product_name,
            "product_id": product_id,
            "unit": unit,
            "price": price,
            "mrp": mrp,
            "super_price": super_price,
            "in_stock": in_stock_flag,
        }
        logger.okay(dict_to_str(product_data), indent=4)
        logger.exit_quiet(not self.verbose)

        return product_data


def test_browser_scraper():
    switcher = ZeptoLocationSwitcher(use_virtual_display=False)
    switcher.set_location(location_idx=2)

    scraper = ZeptoBrowserScraper(use_virtual_display=False)
    # product_id = "14a11cfe-fd72-4901-bf2e-22bc0aba21c0"
    product_id = "7851f4a9-cab6-4b75-bae2-bcbc43bf0bdb"
    product_info = scraper.fetch(product_id, save_cookies=True)
    scraper.dump(product_id, product_info)

    extractor = ZeptoProductDataExtractor(verbose=True)
    extractor.extract(product_info)


if __name__ == "__main__":
    test_browser_scraper()

    # python -m web.zepto.scraper
