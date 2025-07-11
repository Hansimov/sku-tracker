from DrissionPage import Chromium, ChromiumOptions
from DrissionPage._pages.chromium_tab import ChromiumTab
from pyvirtualdisplay import Display
from tclogger import logger, logstr, brk, dict_to_str, dict_get
from time import sleep
from typing import Union

from configs.envs import LOCATIONS
from web.clicker import LocationClicker

# BLINKIT_CONFIG_URL = "https://blinkit.com/config/main"
BLINKIT_FLAG_URL = "https://blinkit.com/api/feature-flags/receive"
BLINKIT_MAP_URL = "https://blinkit.com/mapAPI/autosuggest_google"
BLINKIT_LAYOUT_URL = "https://blinkit.com/v1/layout/product"
BLINKIT_PRN_URL = "https://blinkit.com/prn/x/prid"


class BlinkitBrowserScraper:
    """Install dependencies:
    ```sh
    sudo apt-get install xvfb xserver-xephyr tigervnc-standalone-server x11-utils gnumeric
    pip install pyvirtualdisplay pillow EasyProcess pyautogui mss
    ```

    See: ponty/PyVirtualDisplay: Python wrapper for Xvfb, Xephyr and Xvnc
    * https://github.com/ponty/PyVirtualDisplay
    """

    def __init__(self, use_virtual_display: bool = True):
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
        self.location_clicker = LocationClicker()

    def start_virtual_display(self):
        self.display.start()
        self.is_using_virtual_display = True

    def stop_virtual_display(self):
        if self.is_using_virtual_display:
            self.display.stop()
            self.is_using_virtual_display = False

    def set_location(self, tab: ChromiumTab, location_idx: int = 0):
        location_dict = LOCATIONS[location_idx]
        location_text = location_dict.get("text", "")
        location_shot = location_dict.get("shot", "")
        logger.note(f"  > Setting location:")
        logger.file(f"    * {location_text}")
        location_bar = tab.ele(".^LocationBar__SubtitleContainer")
        location_bar.click()
        location_input = tab.ele('xpath://input[@name="select-locality"]')
        location_input.input(location_text)
        selected_address = tab.ele(".^LocationSearchList__LocationDetailContainer")
        selected_address_label = selected_address.ele(
            ".^LocationSearchList__LocationLabel"
        ).text
        logger.note(f"  > Selected address:")
        logger.okay(f"    * {selected_address_label}")
        self.location_clicker.set_location_image_name(location_shot)
        sleep(2)
        self.location_clicker.run()
        sleep(2)

    def fetch_product_info(
        self, product_id: Union[str, int], location_idx: int = None
    ) -> dict:
        prn_url = f"{BLINKIT_PRN_URL}/{product_id}"
        logger.note(f"> Visiting product page: {logstr.mesg(brk(product_id))}")
        logger.file(f"  * {prn_url}")

        tab = self.browser.latest_tab
        tab.set.load_mode.none()

        layout_url = f"{BLINKIT_LAYOUT_URL}/{product_id}"
        listen_targets = [BLINKIT_FLAG_URL, layout_url]
        tab.listen.start(targets=listen_targets)

        tab.get(prn_url)
        logger.okay(f"  ✓ Title: {brk(tab.title)}")

        logger.note(f"  > Listening targets:")
        for target in listen_targets:
            logger.file(f"    * {target}")

        layout_packet = None
        layout_data = {}
        for packet in tab.listen.steps():
            packet_url = packet.url
            packet_url_str = logstr.file(brk(packet_url))
            if packet_url == BLINKIT_FLAG_URL:
                logger.okay(f"  + Flags packet captured: {packet_url_str}")
                if location_idx is not None:
                    self.set_location(tab, location_idx)
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

        self.stop_virtual_display()
        return layout_data


class BlinkitProductDataExtractor:
    def extract(self, resp: dict) -> dict:
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


def test_browser_scraper():
    scraper = BlinkitBrowserScraper(use_virtual_display=False)
    # product_id = "380156"
    # product_id = "14639"
    product_id = "514893"
    product_info = scraper.fetch_product_info(product_id, location_idx=1)

    extractor = BlinkitProductDataExtractor()
    extractor.extract(product_info)


if __name__ == "__main__":
    test_browser_scraper()

    # python -m web.blinkit.scraper
