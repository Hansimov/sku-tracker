import argparse
import json

from acto import Retrier
from copy import deepcopy
from tclogger import logger, logstr, brk, get_now_str, Runtimer, TCLogbar, TCLogbarGroup
from time import sleep
from pathlib import Path
from typing import Union

from configs.envs import DATA_ROOT, DMART_LOCATIONS
from file.excel_parser import ExcelReader, DataframeParser
from web.dmart.scraper import DmartLocationChecker, DmartLocationSwitcher
from web.dmart.scraper import DmartBrowserScraper, DmartProductDataExtractor
from web.dmart.scraper import url_to_filename
from web.logs import log_link_idx
from file.local_dump import LocalAddressExtractor, DmartProductRespChecker
from cli.arg import BatcherArgParser

WEBSITE_NAME = "dmart"
DMART_INCLUDE_KEYS = [
    "unit",
    "price",
    "mrp",
    "in_stock",
    "location",
]
DMART_KEY_COLUMN_MAP = {
    "unit": "unit size_dmart",
    "price": "price_dmart",
    "mrp": "mrp_dmart",
    "in_stock": "instock_dmart",
    "location": "location_dmart",
}


class DmartScrapeBatcher:
    def __init__(
        self,
        skip_exists: bool = True,
        date_str: str = None,
        close_browser_after_done: bool = True,
    ):
        self.skip_exists = skip_exists
        self.close_browser_after_done = close_browser_after_done
        self.excel_reader = ExcelReader()
        self.switcher = DmartLocationSwitcher()
        self.scraper = DmartBrowserScraper(date_str=date_str)
        self.extractor = DmartProductDataExtractor()
        self.addr_extractor = LocalAddressExtractor(website_name=WEBSITE_NAME)
        self.product_checker = DmartProductRespChecker()
        self.checker = DmartLocationChecker()

    def close_switcher(self):
        try:
            self.switcher.client.close_other_tabs(create_new_tab=True)
        except Exception as e:
            logger.warn(f"× DmartScrapeBatcher.close_switcher: {e}")

    def close_scraper(self):
        try:
            self.scraper.client.close_other_tabs(create_new_tab=True)
            if self.close_browser_after_done:
                self.scraper.client.stop_client(close_browser=True)
        except Exception as e:
            logger.warn(f"× DmartScrapeBatcher.close_scraper: {e}")

    def run(self):
        dmart_links = self.excel_reader.get_column_by_name("weblink_dmart")
        for location_idx, location_item in enumerate(DMART_LOCATIONS):
            location_name = location_item.get("name", "")
            location_text = location_item.get("text", "")
            links = dmart_links[:]
            is_set_location = False
            # multiple runs to scan and recover missing products
            for i in range(3):
                for link_idx, link in enumerate(links):
                    is_log_link_idx = False
                    if not link:
                        # logger.mesg(f"> Skip empty link at row [{link_idx}]")
                        continue
                    product_id = link.split("/")[-1].strip()
                    dump_path = self.scraper.get_dump_path(
                        product_id, parent=location_name
                    )
                    if self.skip_exists and dump_path.exists():
                        location_check = self.addr_extractor.check_dump_path_location(
                            dump_path, correct_location_name=location_name
                        )
                        product_check = self.product_checker.check(dump_path)
                        if location_check and product_check:
                            # logger.note(f"> Skip exists:  {logstr.file(brk(dump_path))}")
                            continue
                        else:
                            if not is_log_link_idx:
                                log_link_idx(link_idx, len(links))
                                is_log_link_idx = True
                            logger.file(f"  * {dump_path}")
                            if not location_check:
                                logger.warn(f"  × Incorrect location")
                            if not product_check:
                                logger.warn(f"  × Incorrect product info")
                            logger.warn(f"  * Remove local dump file, and re-scrape")
                            dump_path.unlink(missing_ok=True)
                    if not is_set_location:
                        logger.hint(
                            f"> New Location: {location_name} ({location_text})"
                        )
                        self.switcher.set_location(location_idx)
                        is_set_location = True
                    if not is_log_link_idx:
                        log_link_idx(link_idx, len(links))
                        is_log_link_idx = True
                    product_info = self.scraper.run(product_id, parent=location_name)
                    self.checker.check_product_location(
                        product_info, location_idx, extra_msg="DmartScrapeBatcher"
                    )
                    extracted_data = self.extractor.extract(product_info)
                    if extracted_data:
                        sleep(2)
        self.close_scraper()


class DmartExtractBatcher:
    def __init__(self, date_str: str = None, verbose: bool = False):
        self.date_str = date_str
        self.verbose = verbose
        self.excel_reader = ExcelReader(verbose=verbose)
        self.extractor = DmartProductDataExtractor()
        self.checker = DmartLocationChecker()
        self.verbose = verbose
        self.init_paths()

    def init_paths(self):
        self.date_str = self.date_str or get_now_str()[:10]
        self.dump_root = DATA_ROOT / "dumps" / self.date_str / WEBSITE_NAME
        self.output_root = DATA_ROOT / "output" / self.date_str / WEBSITE_NAME

    def get_dump_path(self, product_id: Union[str, int], parent: str = None) -> Path:
        filename = f"{url_to_filename(str(product_id))}.json"
        if parent:
            dump_path = self.dump_root / parent / filename
        else:
            dump_path = self.dump_root / filename
        return dump_path

    def get_output_path(self, name: str = None) -> Path:
        if name:
            output_path = self.output_root / f"{self.date_str}_dmart_{name}.xlsx"
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
        dmart_links = self.excel_reader.get_column_by_name("weblink_dmart")
        location_bar = TCLogbar(total=len(DMART_LOCATIONS), head="Location:")
        product_bar = TCLogbar(total=len(dmart_links), head=" * Product:")
        TCLogbarGroup([location_bar, product_bar])
        for location_idx, location_item in enumerate(DMART_LOCATIONS):
            df = deepcopy(self.excel_reader.df)
            df_parser = DataframeParser(df, verbose=self.verbose)
            location_name = location_item.get("name", "")
            links = dmart_links[:]
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
                err_mesg = (
                    f"\n  * dmart.{location_name}.{product_id}: "
                    f"\n  * {logstr.file(product_info_path)}"
                )
                try:
                    self.checker.check_product_location(
                        product_info, location_idx, extra_msg="DmartExtractBatcher"
                    )
                except Exception as e:
                    logger.warn(err_mesg)
                    raise e
                extracted_data = self.extractor.extract(product_info)
                if not extracted_data:
                    logger.warn(err_mesg)
                    logger.warn(f"  × Empty extracted data")
                    continue
                row_dicts.append(extracted_data)
            output_path = self.get_output_path(location_name)
            renamed_row_dicts = df_parser.rename_row_dicts_keys_to_column(
                row_dicts=row_dicts,
                key_column_map=DMART_KEY_COLUMN_MAP,
                include_keys=DMART_INCLUDE_KEYS,
            )
            df_parser.update_df_by_row_dicts(renamed_row_dicts)
            df_parser.dump_to_excel(
                output_path=output_path, sheet_name=output_path.stem
            )
            location_bar.update(increment=1, flush=True)
        print()


def run_scrape_batcher(args: argparse.Namespace):
    try:
        scraper_batcher = DmartScrapeBatcher(
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
        extract_batcher = DmartExtractBatcher(date_str=args.date)
        extract_batcher.run()


if __name__ == "__main__":
    arg_parser = BatcherArgParser()
    args = arg_parser.parse_args()

    with Runtimer():
        main(args)

    # Case 1: Batch scrape
    # python -m web.dmart.batcher -s

    # Case 2: Batch extract
    # python -m web.dmart.batcher -e

    # Case 3: Batch scrape and extract
    # python -m web.dmart.batcher -s -e
