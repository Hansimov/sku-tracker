import argparse
import json

from acto import Retrier
from copy import deepcopy
from tclogger import logger, logstr, brk, get_now_str, Runtimer, TCLogbar, TCLogbarGroup
from time import sleep
from pathlib import Path
from typing import Union

from configs.envs import DATA_ROOT, ZEPTO_LOCATIONS
from file.excel_parser import ExcelReader, DataframeParser
from web.zepto.scraper import ZeptoLocationChecker, ZeptoLocationSwitcher
from web.zepto.scraper import ZeptoBrowserScraper, ZeptoProductDataExtractor
from file.local_dump import LocalAddressExtractor
from cli.arg import BatcherArgParser

WEBSITE_NAME = "zepto"
ZEPTO_INCLUDE_KEYS = [
    "unit",
    "price",
    "price_supersaver",
    "mrp",
    "in_stock",
    "location",
]
ZEPTO_KEY_COLUMN_MAP = {
    "unit": "unit size_zepto",
    "price": "price_zepto",
    "price_supersaver": "price_supersaver_zepto",
    "mrp": "mrp_zepto",
    "in_stock": "instock_zepto",
    "location": "location_zepto",
}


class ZeptoScrapeBatcher:
    def __init__(
        self,
        skip_exists: bool = True,
        date_str: str = None,
        close_browser_after_done: bool = True,
    ):
        self.skip_exists = skip_exists
        self.close_browser_after_done = close_browser_after_done
        self.excel_reader = ExcelReader()
        # NOTE: switcher MUST be placed before scraper
        # as switcher initializes browser with proxy, while scraper not use proxy;
        # in drissionpage, browser is singleton,
        # once the browser is initialized, its proxy could not be set afterwards;
        # so if switcher is placed after scraper,
        # switcher would not work, as scraper is already initiating a browser without proxy
        self.switcher = ZeptoLocationSwitcher()
        self.scraper = ZeptoBrowserScraper(date_str=date_str)
        self.extractor = ZeptoProductDataExtractor()
        self.addr_extractor = LocalAddressExtractor(website_name=WEBSITE_NAME)
        self.checker = ZeptoLocationChecker()

    def close_switcher(self):
        try:
            self.switcher.client.close_other_tabs(create_new_tab=True)
        except Exception as e:
            logger.warn(f"× ZeptoScrapeBatcher.close_switcher: {e}")

    def close_scraper(self):
        try:
            self.scraper.client.close_other_tabs(create_new_tab=True)
            if self.close_browser_after_done:
                self.scraper.client.stop_client(close_browser=True)
        except Exception as e:
            logger.warn(f"× ZeptoScrapeBatcher.close_scraper: {e}")

    def run(self):
        zepto_links = self.excel_reader.get_column_by_name("weblink_zepto")
        for location_idx, location_item in enumerate(ZEPTO_LOCATIONS):
            location_name = location_item.get("name", "")
            location_text = location_item.get("text", "")
            links = zepto_links[:]
            is_set_location = False
            for link_idx, link in enumerate(links):
                if not link:
                    logger.mesg(f"> Skip empty link at row [{link_idx}]")
                    continue
                else:
                    logger.note(
                        f"[{logstr.mesg(link_idx+1)}/{logstr.file(len(links))}]",
                        end=" ",
                    )
                product_id = link.split("/")[-1].strip()
                dump_path = self.scraper.get_dump_path(product_id, parent=location_name)
                if self.skip_exists and dump_path.exists():
                    if self.addr_extractor.check_dump_path_location(
                        dump_path, correct_location_name=location_name
                    ):
                        logger.note(f"> Skip exists:  {logstr.file(brk(dump_path))}")
                        continue
                    else:
                        logger.warn(f"> Remove local dump file, and re-scrape")
                        logger.file(f"  * {dump_path}")
                        dump_path.unlink(missing_ok=True)
                if not is_set_location:
                    logger.hint(f"> New Location: {location_name} ({location_text})")
                    self.switcher.set_location(location_idx)
                    is_set_location = True
                product_info = self.scraper.run(product_id, parent=location_name)
                self.checker.check_product_location(product_info, location_idx)
                extracted_data = self.extractor.extract(product_info)
                if extracted_data:
                    sleep(2)
        self.close_scraper()


class ZeptoExtractBatcher:
    def __init__(self, date_str: str = None, verbose: bool = False):
        self.date_str = date_str
        self.verbose = verbose
        self.excel_reader = ExcelReader(verbose=verbose)
        self.extractor = ZeptoProductDataExtractor()
        self.checker = ZeptoLocationChecker()
        self.verbose = verbose
        self.init_paths()

    def init_paths(self):
        self.date_str = self.date_str or get_now_str()[:10]
        self.dump_root = DATA_ROOT / "dumps" / self.date_str / WEBSITE_NAME
        self.output_root = DATA_ROOT / "output" / self.date_str / WEBSITE_NAME

    def get_dump_path(self, product_id: Union[str, int], parent: str = None) -> Path:
        filename = f"{product_id}.json"
        if parent:
            dump_path = self.dump_root / parent / filename
        else:
            dump_path = self.dump_root / filename
        return dump_path

    def get_output_path(self, name: str = None) -> Path:
        if name:
            output_path = self.output_root / f"{self.date_str}_zepto_{name}.xlsx"
        else:
            output_path = self.output_root / "output.xlsx"
        return output_path

    def load_product_info(
        self, product_id: str, location_name: str = None
    ) -> tuple[dict, Path]:
        logger.enter_quiet(not self.verbose)
        logger.note(f"  > Loading product info: {brk(logstr.mesg(product_id))}")
        product_info = {}
        try:
            product_info_path = self.get_dump_path(
                product_id=product_id, parent=location_name
            )
            with open(product_info_path, "r") as rf:
                product_info = json.load(rf)
        except Exception as e:
            logger.warn(f"  × File not found: {brk(logstr.file(product_info_path))}")
            raise e
        logger.exit_quiet(not self.verbose)
        return product_info, product_info_path

    def run(self):
        zepto_links = self.excel_reader.get_column_by_name("weblink_zepto")
        location_bar = TCLogbar(total=len(ZEPTO_LOCATIONS), head="Location:")
        product_bar = TCLogbar(total=len(zepto_links), head=" * Product:")
        TCLogbarGroup([location_bar, product_bar])
        for location_idx, location_item in enumerate(ZEPTO_LOCATIONS):
            df = deepcopy(self.excel_reader.df)
            df_parser = DataframeParser(df, verbose=self.verbose)
            location_name = location_item.get("name", "")
            links = zepto_links[:]
            location_bar.update(desc=logstr.mesg(brk(location_name)), flush=True)
            row_dicts: list[dict] = []
            for link_idx, link in enumerate(links):
                product_bar.update(increment=1)
                if not link:
                    logger.mesg(
                        f"  * Skip empty link at row [{link_idx}]", verbose=self.verbose
                    )
                    row_dicts.append({})
                    continue
                product_id = link.split("/")[-1].strip()
                product_bar.set_desc(logstr.mesg(brk(product_id)))
                product_info, product_info_path = self.load_product_info(
                    product_id=product_id, location_name=location_name
                )
                try:
                    self.checker.check_product_location(
                        product_info, location_idx, extra_msg="ZeptoExtractBatcher"
                    )
                except Exception as e:
                    logger.warn(
                        f"    * zepto.{location_name}.{product_id}: "
                        f"{logstr.file(brk(product_info_path))}"
                    )
                    raise e
                extracted_data = self.extractor.extract(product_info)
                row_dicts.append(extracted_data)
            output_path = self.get_output_path(location_name)
            renamed_row_dicts = df_parser.rename_row_dicts_keys_to_column(
                row_dicts=row_dicts,
                key_column_map=ZEPTO_KEY_COLUMN_MAP,
                include_keys=ZEPTO_INCLUDE_KEYS,
            )
            df_parser.update_df_by_row_dicts(renamed_row_dicts)
            df_parser.dump_to_excel(
                output_path=output_path, sheet_name=output_path.stem
            )
            location_bar.update(increment=1, flush=True)
        print()


def run_scrape_batcher(args: argparse.Namespace):
    try:
        scraper_batcher = ZeptoScrapeBatcher(
            skip_exists=not args.force_scrape,
            date_str=args.date,
            close_browser_after_done=args.close_browser_after_done,
        )
        scraper_batcher.run()
    except Exception as e:
        logger.warn(e)
        logger.warn(f"> Closing tabs ...")
        sleep(5)
        scraper_batcher.close_scraper()
        raise e


def main(args: argparse.Namespace):
    if args.scrape:
        with Retrier(max_retries=30, retry_interval=60) as retrier:
            retrier.run(run_scrape_batcher, args=args)

    if args.extract:
        extract_batcher = ZeptoExtractBatcher(date_str=args.date)
        extract_batcher.run()


if __name__ == "__main__":
    arg_parser = BatcherArgParser()
    args = arg_parser.parse_args()

    with Runtimer():
        main(args)

    # Case 1: Batch scrape
    # python -m web.zepto.batcher -s

    # Case 2: Batch extract
    # python -m web.zepto.batcher -e

    # Case 3: Batch scrape and extract
    # python -m web.zepto.batcher -s -e
