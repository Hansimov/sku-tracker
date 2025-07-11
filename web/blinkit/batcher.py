from tclogger import logger, logstr, Runtimer
from time import sleep

from configs.envs import LOCATIONS
from file.excel_parser import ExcelReader
from web.blinkit.scraper import BlinkitBrowserScraper, BlinkitProductDataExtractor


class BlinkitBatcher:
    def __init__(self):
        self.excel_reader = ExcelReader()
        self.scraper = BlinkitBrowserScraper(use_virtual_display=False)
        self.extractor = BlinkitProductDataExtractor()

    def run(self):
        blinkit_links = self.excel_reader.get_column_by_name("weblink_blinkit")
        for location_idx, location_item in enumerate(LOCATIONS):
            self.scraper.new_tab()
            location_name = location_item.get("name", "")
            location_text = location_item.get("text", "")
            is_set_location = False
            logger.hint(f"> New Location: {location_name} ({location_text})")
            links = blinkit_links[:]
            links_count = len(links)
            for link_idx, link in enumerate(links):
                if not link:
                    logger.mesg(f"> Skip empty link at row [{link_idx}]")
                    continue
                else:
                    logger.note(
                        f" > [{logstr.mesg(link_idx+1)}/{logstr.file(links_count)}]",
                        end=" ",
                    )
                product_id = link.split("/")[-1]
                if not is_set_location:
                    location_idx = location_idx
                    is_set_location = True
                else:
                    location_idx = None
                product_info = self.scraper.run(
                    product_id,
                    location_idx=location_idx,
                    save_cookies=True,
                    parent=location_name,
                )
                extracted_data = self.extractor.extract(product_info)
                sleep(0.5)


if __name__ == "__main__":
    with Runtimer():
        batcher = BlinkitBatcher()
        batcher.run()

    # python -m web.blinkit.batcher
