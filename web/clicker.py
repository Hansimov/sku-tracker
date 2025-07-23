import cv2

from DrissionPage._pages.chromium_tab import ChromiumTab
from pathlib import Path
from time import sleep

from configs.envs import IMGS_ROOT


class ImageMatcher:
    def __init__(self, source_image_path: Path, template_image_path: Path):
        self.source_image = cv2.imread(str(source_image_path))
        self.template_image = cv2.imread(str(template_image_path))
        self.detected_image_path = (
            source_image_path.parent / f"{source_image_path.stem}_detected.png"
        )

    def match(self):
        """
        OpenCV: Template Matching
        * https://docs.opencv.org/3.4/de/da9/tutorial_template_matching.html

        Return: (left, top, right, bottom)
        """
        res = cv2.matchTemplate(
            self.source_image, self.template_image, cv2.TM_CCOEFF_NORMED
        )
        _, _, _, (left, top) = cv2.minMaxLoc(res)
        right = left + self.template_image.shape[1]
        bottom = top + self.template_image.shape[0]
        self.match_region = (left, top, right, bottom)
        return self.match_region

    def draw_rectangle(self):
        cv2.rectangle(
            img=self.source_image,
            pt1=self.match_region[:2],
            pt2=self.match_region[2:],
            color=(0, 255, 0),  # BGR
            thickness=2,
        )
        cv2.imwrite(str(self.detected_image_path), self.source_image)


class LocationClicker:
    def __init__(self, tab: ChromiumTab = None, suffix: str = ""):
        self.tab = tab
        self.suffix = suffix
        self.init_paths()

    def init_paths(self):
        screenshot_name = "screenshot"
        if self.suffix:
            screenshot_name = f"{screenshot_name}_{self.suffix}"
        self.screenshot_image_path = IMGS_ROOT / f"{screenshot_name}.png"

    def set_location_image_name(self, location_image_name: str):
        self.location_image_name = location_image_name
        self.location_image_path = IMGS_ROOT / self.location_image_name

    def get_screenshot(self):
        self.tab.get_screenshot(path=self.screenshot_image_path, full_page=False)

    def get_location_item_position(self):
        self.get_screenshot()
        matcher = ImageMatcher(
            source_image_path=self.screenshot_image_path,
            template_image_path=self.location_image_path,
        )
        left, top, right, bottom = matcher.match()
        matcher.draw_rectangle()
        center_x = (left + right) / 2
        center_y = (top + bottom) / 2
        return center_x, center_y

    def click_target_position(self):
        x, y = self.get_location_item_position()
        for event in ("mousePressed", "mouseReleased"):
            self.tab.run_cdp(
                "Input.dispatchMouseEvent",
                type=event,
                x=x,
                y=y,
                button="left",
                clickCount=1,
            )


class BlinkitLocationClicker(LocationClicker):
    def __init__(self, tab: ChromiumTab, suffix: str = "blinkit"):
        super().__init__(tab=tab, suffix=suffix)


class SwiggyLocationClicker(LocationClicker):
    def __init__(self, tab: ChromiumTab, suffix: str = "swiggy"):
        super().__init__(tab=tab, suffix=suffix)

    def type_target_location_text(self, location_text: str):
        # select input field with triple clicks
        x, y = self.get_location_item_position()
        for event in ("mousePressed", "mouseReleased"):
            self.tab.run_cdp(
                "Input.dispatchMouseEvent",
                type=event,
                x=x,
                y=y,
                button="left",
                clickCount=3,
            )

        # delete existing text
        BACKSPACE_KEY_CODE = 8
        for event in ("keyDown", "keyUp"):
            self.tab.run_cdp(
                "Input.dispatchKeyEvent",
                type=event,
                windowsVirtualKeyCode=BACKSPACE_KEY_CODE,
            )

        # type location text
        for ch in location_text:
            self.tab.run_cdp("Input.dispatchKeyEvent", type="char", text=ch)
            sleep(0.05)
