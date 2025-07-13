import argparse
import json
import sys

from copy import deepcopy
from tclogger import logger, logstr, brk, Runtimer, TCLogbar, TCLogbarGroup
from tclogger import dict_to_str, dict_get, get_now_str
from time import sleep
from pathlib import Path
from typing import Union

from configs.envs import DATA_ROOT, SWIGGY_LOCATIONS
from file.excel_parser import ExcelReader, DataframeParser
from web.swiggy.scraper import SwiggyLocationChecker, SwiggyLocationSwitcher
from web.swiggy.scraper import SwiggyBrowserScraper, SwiggyProductDataExtractor

SWIGGY_INCLUDE_KEYS = ["unit", "price", "mrp", "in_stock"]
SWIGGY_KEY_COLUMN_MAP = {
    "unit": "unit size_instamart",
    "price": "price_instamart",
    "mrp": "mrp_instamart",
    "in_stock": "instock_instamart",
}


class SwiggyScrapeBatcher:
    def __init__(self):
        self.excel_reader = ExcelReader()
        self.scraper = SwiggyBrowserScraper(use_virtual_display=False)
        self.switcher = SwiggyLocationSwitcher(use_virtual_display=False)
        self.extractor = SwiggyProductDataExtractor()
        self.checker = SwiggyLocationChecker()

    def run(self, skip_exists: bool = True):
        swiggy_links = self.excel_reader.get_column_by_name("weblink_instamart")
        for location_idx, location_item in enumerate(SWIGGY_LOCATIONS):
            location_name = location_item.get("name", "")
            location_text = location_item.get("text", "")
            links = swiggy_links[:]
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
                if skip_exists and dump_path.exists():
                    logger.note(f"  > Skip exists:")
                    logger.file(f"    * {dump_path}")
                    continue
                if not is_set_location:
                    logger.hint(f"> New Location: {location_name} ({location_text})")
                    self.scraper.new_tab()
                    self.switcher.set_location(location_idx)
                    is_set_location = True
                product_info = self.scraper.run(product_id, parent=location_name)
                self.checker.check_product_location(
                    product_info, location_idx, extra_msg="SwiggyScrapeBatcher"
                )
                extracted_data = self.extractor.extract(product_info)
                if extracted_data:
                    sleep(3)


class SwiggyExtractBatcher:
    def __init__(self, verbose: bool = False):
        self.excel_reader = ExcelReader(verbose=verbose)
        self.extractor = SwiggyProductDataExtractor()
        self.checker = SwiggyLocationChecker()
        self.verbose = verbose
        self.init_paths()

    def init_paths(self):
        date_str = get_now_str()[:10]
        self.dump_root = DATA_ROOT / "dumps" / date_str / "swiggy"
        self.output_root = DATA_ROOT / "output" / date_str / "swiggy"
        self.date_str = date_str

    def get_dump_path(self, product_id: Union[str, int], parent: str = None) -> Path:
        filename = f"{product_id}.json"
        if parent:
            dump_path = self.dump_root / parent / filename
        else:
            dump_path = self.dump_root / filename
        return dump_path

    def get_output_path(self, name: str = None) -> Path:
        if name:
            output_path = self.output_root / f"{self.date_str}_swiggy_{name}.xlsx"
        else:
            output_path = self.output_root / "output.xlsx"
        return output_path

    def load_product_info(self, product_id: str, location_name: str = None) -> dict:
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
        return product_info

    def run(self):
        swiggy_links = self.excel_reader.get_column_by_name("weblink_instamart")
        location_bar = TCLogbar(total=len(SWIGGY_LOCATIONS), head="Location:")
        product_bar = TCLogbar(total=len(swiggy_links), head=" * Product:")
        TCLogbarGroup([location_bar, product_bar])
        for location_idx, location_item in enumerate(SWIGGY_LOCATIONS):
            df = deepcopy(self.excel_reader.df)
            df_parser = DataframeParser(df, verbose=self.verbose)
            location_name = location_item.get("name", "")
            links = swiggy_links[:]
            location_bar.update(desc=logstr.mesg(brk(location_name)), flush=True)
            row_dicts: list[dict] = []
            for link_idx, link in enumerate(links):
                product_bar.update(increment=1)
                if not link:
                    logger.mesg(f"  * Skip empty link at row [{link_idx}]")
                    row_dicts.append({})
                    continue
                product_id = link.split("/")[-1].strip()
                product_bar.set_desc(logstr.mesg(brk(product_id)))
                product_info = self.load_product_info(
                    product_id=product_id, location_name=location_name
                )
                try:
                    self.checker.check_product_location(
                        product_info, location_idx, extra_msg="SwiggyExtractBatcher"
                    )
                except Exception as e:
                    logger.warn(f"    * {product_id}")
                    raise e
                extracted_data = self.extractor.extract(product_info)
                row_dicts.append(extracted_data)
            output_path = self.get_output_path(location_name)
            renamed_row_dicts = df_parser.rename_row_dicts_keys_to_column(
                row_dicts=row_dicts,
                key_column_map=SWIGGY_KEY_COLUMN_MAP,
                include_keys=SWIGGY_INCLUDE_KEYS,
            )
            df_parser.update_df_by_row_dicts(renamed_row_dicts)
            df_parser.dump_to_excel(
                output_path=output_path, sheet_name=output_path.stem
            )
            location_bar.update(increment=1, flush=True)
        print()


class SwiggyBatcherArgParser(argparse.ArgumentParser):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.add_argument("-s", "--scrape", action="store_true")
        self.add_argument("-e", "--extract", action="store_true")

    def parse_args(self):
        self.args, self.unknown_args = self.parse_known_args(sys.argv[1:])
        return self.args


def main(args: argparse.Namespace):
    if args.scrape:
        scraper_batcher = SwiggyScrapeBatcher()
        scraper_batcher.run()

    if args.extract:
        extract_batcher = SwiggyExtractBatcher()
        extract_batcher.run()

    if not (args.scrape or args.extract):
        logger.warn("No valid argument: `-s` for scrape or `-e` for extract.")


if __name__ == "__main__":
    arg_parser = SwiggyBatcherArgParser()
    args = arg_parser.parse_args()
    with Runtimer():
        main(args)

    # Case 1: Batch scrape
    # python -m web.swiggy.batcher -s

    # Case 2: Batch extract
    # python -m web.swiggy.batcher -e
