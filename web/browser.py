from DrissionPage import Chromium, ChromiumOptions
from DrissionPage._pages.chromium_tab import ChromiumTab
from pyvirtualdisplay import Display
from tclogger import logger


class BrowserClient:
    def __init__(self, use_virtual_display: bool = False):
        self.use_virtual_display = use_virtual_display
        self.is_using_virtual_display = False
        self.is_browser_opened = False

    def open_virtual_display(self):
        if self.use_virtual_display and not self.is_using_virtual_display:
            self.display = Display()
            self.display.start()
            self.is_using_virtual_display = True

    def close_virtual_display(self):
        if self.is_using_virtual_display and hasattr(self, "display"):
            self.display.stop()
        self.is_using_virtual_display = False

    def open_browser(self):
        if self.is_browser_opened:
            return
        chrome_options = ChromiumOptions()
        self.browser = Chromium(addr_or_opts=chrome_options)
        self.chrome_options = chrome_options
        self.is_browser_opened = True

    def close_browser(self):
        if hasattr(self, "browser") and self.is_browser_opened:
            try:
                self.browser.quit()
            except Exception as e:
                logger.warn(f"× BrowserClient.close_browser: {e}")
            self.is_browser_opened = False

    def start_client(self):
        self.open_virtual_display()
        self.open_browser()

    def stop_client(self, close_browser: bool = False):
        if close_browser:
            self.close_browser()
        self.close_virtual_display()

    def close_other_tabs(self, create_new_tab: bool = True):
        if hasattr(self, "browser") and isinstance(self.browser, Chromium):
            if create_new_tab:
                self.browser.new_tab()
            self.browser.latest_tab.close(others=True)
