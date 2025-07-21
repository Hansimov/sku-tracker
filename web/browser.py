from DrissionPage import Chromium, ChromiumOptions
from DrissionPage._pages.chromium_tab import ChromiumTab
from pyvirtualdisplay import Display
from tclogger import logger


class BrowserClient:
    """Install dependencies:
    ```sh
    sudo apt-get install xvfb xserver-xephyr tigervnc-standalone-server x11-utils gnumeric
    pip install pyvirtualdisplay pillow EasyProcess pyautogui mss
    ```

    See: ponty/PyVirtualDisplay: Python wrapper for Xvfb, Xephyr and Xvnc
    * https://github.com/ponty/PyVirtualDisplay

    See also: [Bug]: Missing X server or $DISPLAY · Issue #8148 · puppeteer/puppeteer
        * https://github.com/puppeteer/puppeteer/issues/8148

    ```sh
    # xdpyinfo -display :10.0
    export DISPLAY=localhost:10.0
    ```
    """

    def __init__(self, use_virtual_display: bool = False, proxy: str = None):
        self.use_virtual_display = use_virtual_display
        self.proxy = proxy
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
        if self.proxy:
            chrome_options.set_proxy(self.proxy)
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


if __name__ == "__main__":
    client = BrowserClient(use_virtual_display=False)
    client.start_client()
    tab = client.browser.new_tab()
    client.stop_client(close_browser=True)

    # python -m web.browser
