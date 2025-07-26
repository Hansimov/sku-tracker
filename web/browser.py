from DrissionPage import Chromium, ChromiumOptions
from pyvirtualdisplay import Display
from tclogger import logger, dict_to_str
from typing import Union, TypedDict, Optional

from configs.envs import CHROME_USER_DATA_DIR


class BrowserSettingType(TypedDict):
    uid: Optional[Union[int, str]]
    port: Optional[Union[int, str]]
    proxy: Optional[str]
    use_virtual_display: Optional[bool]


class BrowserClient:
    def __init__(
        self,
        uid: Union[int, str] = None,
        port: Union[int, str] = None,
        proxy: str = None,
        use_virtual_display: bool = False,
    ):
        self.use_virtual_display = use_virtual_display
        self.proxy = proxy
        self.port = port
        self.uid = uid
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
        logger.note("> Opening browser ...")
        info_dict = {}
        chrome_options = ChromiumOptions()
        if self.uid:
            self.user_data_path = CHROME_USER_DATA_DIR / str(self.uid)
            chrome_options.set_user_data_path(self.user_data_path)
            info_dict["user_data_path"] = str(self.user_data_path)
        if self.port:
            chrome_options.set_local_port(self.port)
            info_dict["port"] = self.port
        if self.proxy:
            chrome_options.set_proxy(self.proxy)
            info_dict["proxy"] = self.proxy
        if info_dict:
            logger.mesg(dict_to_str(info_dict), indent=2)
        self.chrome_options = chrome_options
        self.browser = Chromium(addr_or_opts=self.chrome_options)
        self.is_browser_opened = True

    def close_browser(self):
        if hasattr(self, "browser") and self.is_browser_opened:
            logger.note(f"> Closing browser ...")
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
