import json

from DrissionPage._pages.chromium_tab import ChromiumTab
from pathlib import Path
from tclogger import logger, logstr, brk, dict_to_str, dict_get, dict_set, get_now_str
from time import sleep
from typing import Union
from urllib.parse import unquote

from configs.envs import DATA_ROOT, BLINKIT_LOCATIONS
from web.clicker import LocationClicker
from web.browser import BrowserClient
from web.fetch import fetch_with_retry
from file.local_dump import LocalAddressExtractor

WEBSITE_NAME = "blinkit"
BLINKIT_MAIN_URL = "https://blinkit.com"
BLINKIT_FLAG_URL = "https://blinkit.com/api/feature-flags/receive"
BLINKIT_MAP_URL = "https://blinkit.com/mapAPI/autosuggest_google"
BLINKIT_LAYOUT_URL = "https://blinkit.com/v1/layout/product"
BLINKIT_PRN_URL = "https://blinkit.com/prn/x/prid"


class BlinkitLocationChecker:
    def get_correct_address(self, location_idx: int) -> str:
        return BLINKIT_LOCATIONS[location_idx].get("locality", "")

    def unify_address(self, address: str) -> str:
        if address:
            return unquote(address).lower()
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
    ) -> bool:
        cookies_dict = tab.cookies(all_info=True).as_dict()
        local_address = cookies_dict.get("gr_1_locality", "")
        correct_address = self.get_correct_address(location_idx)
        return self.check_address(
            local_address, correct_address, extra_msg, raise_error=False
        )

    def check_product_location(
        self, product_info: dict, location_idx: int, extra_msg: str = ""
    ):
        product_address = dict_get(product_info, "cookies.gr_1_locality", "")
        correct_address = self.get_correct_address(location_idx)
        return self.check_address(product_address, correct_address, extra_msg)


class BlinkitLocationSwitcher:
    def __init__(self):
        self.checker = BlinkitLocationChecker()
        self.client = BrowserClient()

    def create_clicker(self):
        self.clicker = LocationClicker()

    def set_location(self, location_idx: int = 0) -> dict:
        self.client.start_client()
        self.create_clicker()
        tab = self.client.browser.latest_tab
        tab.set.load_mode.none()

        tab.get(BLINKIT_MAIN_URL)
        logger.mesg(f"  ✓ Title: {brk(tab.title)}")

        if self.checker.check_tab_location(
            tab, location_idx, extra_msg="BlinkitLocationSwitcher"
        ):
            logger.okay("  * Location already correctly set. Skip.")
        else:
            logger.note(f"  > Setting location:")
            location_dict = BLINKIT_LOCATIONS[location_idx]
            location_text = location_dict.get("text", "")
            location_shot = location_dict.get("shot", "")
            logger.file(f"    * {location_text}")
            location_bar = tab.ele(".^LocationBar__SubtitleContainer")
            sleep(2)
            location_bar.click()
            sleep(2)
            location_input = tab.ele('xpath://input[@name="select-locality"]')
            location_input.input(location_text)
            sleep(2)
            selected_address = tab.ele(".^LocationSearchList__LocationDetailContainer")
            selected_address_label = selected_address.ele(
                ".^LocationSearchList__LocationLabel"
            ).text
            logger.note(f"  > Selected address: {logstr.okay(selected_address_label)}")
            self.clicker.set_location_image_name(location_shot)
            sleep(2)
            self.clicker.click_target_position()
            sleep(10)

        # self.client.close_other_tabs(create_new_tab=True)
        self.client.stop_client(close_browser=False)


class BlinkitBrowserScraper:
    def __init__(self, date_str: str = None):
        self.date_str = date_str
        self.client = BrowserClient()
        self.checker = BlinkitLocationChecker()
        self.init_paths()

    def create_clicker(self):
        self.clicker = LocationClicker()

    def init_paths(self):
        self.date_str = self.date_str or get_now_str()[:10]
        self.dump_root = DATA_ROOT / "dumps" / self.date_str / WEBSITE_NAME

    def get_cookies(self, tab: ChromiumTab) -> dict:
        cookies_dict = tab.cookies(all_info=True).as_dict()
        cookies_dict["url"] = tab.url
        cookies_dict["now"] = get_now_str()
        return cookies_dict

    def clean_resp(self, resp: dict) -> dict:
        dict_set(resp, ["response", "page_actions"], [])
        dict_set(resp, ["response", "page_level_components"], {})
        dict_set(resp, ["response", "snippet_list_updater_data"], {})

        clean_snippets = []
        snippets_keys = ["response", "snippets"]
        snippets = dict_get(resp, snippets_keys, [])
        for snippet in snippets:
            if snippet.get("widget_type") in ["product_atc_strip"]:
                clean_snippets.append(snippet)
        dict_set(resp, snippets_keys, clean_snippets)

        clean_attributes = []
        atttributes_keys = [
            *["response", "tracking", "le_meta"],
            *["custom_data", "seo", "attributes"],
        ]
        attributes = dict_get(resp, atttributes_keys, [])
        for attr in attributes:
            if attr.get("name", "").lower() in ["unit"]:
                clean_attributes.append(attr)
        dict_set(resp, atttributes_keys, clean_attributes)
        return resp

    def fetch(self, product_id: Union[str, int], save_cookies: bool = True) -> dict:
        prn_url = f"{BLINKIT_PRN_URL}/{product_id}"
        logger.note(f"> Visiting product page: {logstr.mesg(brk(product_id))}")
        logger.file(f"  * {prn_url}")

        self.client.start_client()
        tab = self.client.browser.latest_tab
        tab.set.load_mode.none()

        layout_url = f"{BLINKIT_LAYOUT_URL}/{product_id}"
        listen_targets = [BLINKIT_FLAG_URL, layout_url]
        tab.listen.start(targets=listen_targets)

        tab.get(prn_url)
        logger.mesg(f"  ✓ Title: {brk(tab.title)}")

        logger.note(f"  > Listening targets:")
        for target in listen_targets:
            logger.file(f"    * {target}")

        layout_packet = None
        layout_data = {}
        for packet in tab.listen.steps(timeout=30):
            packet_url = packet.url
            packet_url_str = logstr.file(brk(packet_url))
            if packet_url == BLINKIT_FLAG_URL:
                logger.okay(f"  + Flags packet captured: {packet_url_str}")
            elif packet_url == layout_url:
                logger.okay(f"  + Layout packet captured: {packet_url_str}")
                layout_packet = packet
                tab.stop_loading()
                break
            else:
                logger.warn(f"  × Unexpected packet: {packet_url_str}")

        if layout_packet:
            layout_resp = layout_packet.response
            if layout_resp:
                layout_data = layout_resp.body
                layout_data = self.clean_resp(layout_data)

        if layout_data and save_cookies:
            layout_data["cookies"] = self.get_cookies(tab)

        self.client.stop_client(close_browser=False)
        return layout_data

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


class BlinkitProductDataExtractor:
    def __init__(self, verbose: bool = False):
        self.verbose = verbose
        self.address_extractor = LocalAddressExtractor(website_name=WEBSITE_NAME)

    def extract(self, resp: dict) -> dict:
        logger.enter_quiet(not self.verbose)
        if not resp:
            logger.warn("  × Empty response data to extract")
            logger.exit_quiet(not self.verbose)
            return {}

        logger.note(f"  > Extracting product Data ...")

        # get in_stock
        snippets = dict_get(resp, ["response", "snippets"], [])
        atc_strip_data = {}
        for snippet in snippets:
            if snippet.get("widget_type") == "product_atc_strip":
                atc_strip_data = snippet.get("data", {})
                break
        product_state = dict_get(atc_strip_data, ["product_state"], "").lower()
        if product_state == "available":
            in_stock = 1
        elif product_state == "out_of_stock":
            in_stock = 0
        else:
            in_stock = "N/A"

        # get product_name, price, mrp, unit
        meta_data = dict_get(resp, ["response", "tracking", "le_meta"], {})
        seo_data = dict_get(meta_data, ["custom_data", "seo"], {})
        product_id = dict_get(meta_data, "id", None)
        product_name = dict_get(seo_data, ["product_name"], None)
        price = dict_get(seo_data, ["price"], None)
        mrp = dict_get(seo_data, ["mrp"], None)
        unit = None
        for attr in seo_data.get("attributes", []):
            if attr.get("name", "").lower() == "unit":
                unit = attr.get("value")
                break

        # get location
        location = self.address_extractor.get_column_location(resp)

        product_data = {
            "product_name": product_name,
            "product_id": product_id,
            "in_stock": in_stock,
            "price": price,
            "mrp": mrp,
            "unit": unit,
            "location": location,
        }
        logger.okay(dict_to_str(product_data), indent=4)
        logger.exit_quiet(not self.verbose)

        return product_data


def test_browser_scraper():
    switcher = BlinkitLocationSwitcher()
    switcher.set_location(location_idx=0)
    sleep(2)

    scraper = BlinkitBrowserScraper()
    # product_id = "380156"
    # product_id = "14639"
    product_id = "514893"
    product_info = scraper.fetch(product_id, save_cookies=True)
    scraper.dump(product_id, product_info)

    extractor = BlinkitProductDataExtractor(verbose=True)
    extractor.extract(product_info)


if __name__ == "__main__":
    test_browser_scraper()

    # python -m web.blinkit.scraper
