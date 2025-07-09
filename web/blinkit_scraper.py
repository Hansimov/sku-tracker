import urllib.parse

from DrissionPage import Chromium, ChromiumOptions, SessionPage
from tclogger import logger, logstr, brk, dict_to_str, dict_get
from typing import Union
from pyvirtualdisplay import Display


USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/138.0.0.0 Safari/537.36"

BLINKIT_CONFIG_URL = "https://blinkit.com/config/main"
BLINKIT_FLAG_URL = "https://blinkit.com/api/feature-flags/receive"
BLINKIT_MAP_URL = "https://blinkit.com/mapAPI/autosuggest_google"
BLINKIT_PAGE_URL = "https://blinkit.com/prn/x/prid"
BLINKIT_LAYOUT_URL = "https://blinkit.com/v1/layout/product"
BLINKIT_PRN_URL = "https://blinkit.com/prn/x/prid"


class BlinkitSessionScraper:
    def __init__(self):
        self.page = SessionPage()
        self.sess = self.page.session

    def fetch_config(self) -> dict:
        logger.note(f"> Fetching Blinkit config:")
        logger.file(f"  * {BLINKIT_CONFIG_URL}")
        request_headers = {}
        self.page.get(BLINKIT_CONFIG_URL, headers=request_headers)
        resp_json = self.page.response.json()
        logger.mesg(resp_json)
        return resp_json

    def fetch_feature_flags(self) -> dict:
        logger.note(f"> Fetching feature flags:")
        logger.file(f"  * {BLINKIT_FLAG_URL}")
        request_headers = {}
        self.page.get(BLINKIT_FLAG_URL, headers=request_headers)
        resp_json = self.page.response.json()
        logger.mesg(resp_json)
        return resp_json

    def fetch_map_data(self, query: str) -> dict:
        quoted_query = urllib.parse.quote(query)
        logger.note(f"> Fetching map data:")
        logger.file(f"  * {quoted_query}")
        request_params = {
            "query": query,
        }
        request_headers = {}
        self.page.get(BLINKIT_MAP_URL, params=request_params, headers=request_headers)
        resp_json = self.page.response.json()
        logger.mesg(resp_json)
        return resp_json

    def fetch_layout_data(self, product_id: Union[str, int]) -> dict:
        layout_url = f"{BLINKIT_LAYOUT_URL}/{product_id}"
        logger.note(f"> Fetching product layout: {logstr.mesg(brk(product_id))}")
        logger.file(f"  * {layout_url}")
        self.page.post(layout_url)
        resp_json = self.page.response.json()
        logger.mesg(resp_json)
        return resp_json


class BlinkitBrowserScraper:
    """ponty/PyVirtualDisplay: Python wrapper for Xvfb, Xephyr and Xvnc
    * https://github.com/ponty/PyVirtualDisplay

    Install dependencies:

    ```sh
    sudo apt-get install xvfb xserver-xephyr tigervnc-standalone-server x11-utils gnumeric
    pip install pyvirtualdisplay pillow EasyProcess
    ```
    """

    def __init__(self):
        self.init_virtual_display()
        self.init_browser()

    def init_virtual_display(self):
        self.display = Display()
        self.start_virtual_display()

    def init_browser(self):
        chrome_options = ChromiumOptions()
        self.browser = Chromium(addr_or_opts=chrome_options)
        self.chrome_options = chrome_options

    def start_virtual_display(self):
        self.display.start()
        self.is_using_virtual_display = True

    def stop_virtual_display(self):
        if self.is_using_virtual_display:
            self.display.stop()
            self.is_using_virtual_display = False

    def fetch_product_info(self, product_id: Union[str, int]) -> dict:
        prn_url = f"{BLINKIT_PRN_URL}/{product_id}"
        logger.note(f"> Visiting product page: {logstr.mesg(brk(product_id))}")
        logger.file(f"  * {prn_url}")
        tab = self.browser.latest_tab
        tab.set.load_mode.none()
        layout_url = f"{BLINKIT_LAYOUT_URL}/{product_id}"
        tab.listen.start(layout_url)
        tab.get(prn_url)
        logger.okay(f"  ✓ Title: {brk(tab.title)}")
        logger.note(f"  > Listening: {layout_url}")
        packet = tab.listen.wait()
        tab.stop_loading()
        logger.okay(f"    ✓ Packet: {packet.url}")
        packet_resp = packet.response
        if packet_resp:
            packet_data = packet_resp.body
            # logger.okay(dict_to_str(packet_data))
        else:
            packet_data = {}
            logger.warn(f"  × No response of layout packet")

        self.stop_virtual_display()
        return packet_data

    def extract_product_data(self, resp: dict) -> dict:
        logger.note(f"  > Extracting product Data ...")

        # get in_stock
        snippes = dict_get(resp, ["response", "snippets"], [])
        atc_strip_data = {}
        for snippet in snippes:
            if snippet.get("widget_type") == "product_atc_strip":
                atc_strip_data = snippet.get("data", {})
                break
        product_state = dict_get(atc_strip_data, ["product_state"], "").lower()
        if product_state == "available":
            in_stock = True
        elif product_state == "out_of_stock":
            in_stock = False
        else:
            in_stock = None

        # get product_name, price, mrp, unit
        meta_data = dict_get(resp, ["response", "tracking", "le_meta"], {})
        seo_data = dict_get(meta_data, ["custom_data", "seo"], {})
        product_name = dict_get(seo_data, ["product_name"], None)
        price = dict_get(seo_data, ["price"], None)
        mrp = dict_get(seo_data, ["mrp"], None)
        unit = None
        for attr in seo_data.get("attributes", []):
            if attr.get("name", "").lower() == "unit":
                unit = attr.get("value")
                break

        product_data = {
            "product_name": product_name,
            "in_stock": in_stock,
            "price": price,
            "mrp": mrp,
            "unit": unit,
        }
        logger.okay(dict_to_str(product_data), indent=4)
        return product_data


def test_session_scraper():
    scraper = BlinkitSessionScraper()
    # Fetch config
    scraper.fetch_config()
    # Fetch feature flags
    scraper.fetch_feature_flags()
    # Fetch location
    query = "Mumbai"
    scraper.fetch_map_data(query)
    # Fetch product ID
    product_id = "380156"
    scraper.fetch_layout_data(product_id)


def test_browser_scraper():
    scraper = BlinkitBrowserScraper()
    # Fetch product data
    # product_id = "380156"
    product_id = "14639"
    # product_id = "514893"
    product_info = scraper.fetch_product_info(product_id)
    scraper.extract_product_data(product_info)


if __name__ == "__main__":
    # test_session_scraper()
    test_browser_scraper()

    # python -m web.blinkit_scraper
