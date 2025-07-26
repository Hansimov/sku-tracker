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


def test_browser_scraper():
    # switcher = DmartLocationSwitcher()
    # switcher.set_location(location_idx=0)

    scraper = DmartBrowserScraper()
    product_id = "fortune-chakki-fresh-atta-patta0fort45xx160320?selectedProd=713128"
    product_info = scraper.fetch(product_id, save_cookies=True)
    scraper.dump(product_id, product_info)

    # extractor = DmartProductDataExtractor(verbose=True)
    # extractor.extract(product_info)


if __name__ == "__main__":
    test_browser_scraper()

    # python -m web.dmart.scraper
