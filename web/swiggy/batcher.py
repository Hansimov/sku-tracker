import argparse
import json
import pandas as pd
import shutil

from acto import Retrier
from copy import deepcopy
from tclogger import logger, logstr, brk, get_now_str, Runtimer, TCLogbar, TCLogbarGroup
from tclogger import match_val
from time import sleep
from pathlib import Path
from typing import Union

from configs.envs import DATA_ROOT, SWIGGY_LOCATIONS
from file.excel_parser import ExcelReader, DataframeParser
from web.swiggy.scraper import SwiggyLocationChecker, SwiggyLocationSwitcher
from web.swiggy.scraper import SwiggyBrowserScraper, SwiggyProductDataExtractor
from web.blinkit.batcher import BlinkitExtractBatcher
from web.zepto.batcher import ZeptoExtractBatcher
from file.local_dump import LocalAddressExtractor
from cli.arg import BatcherArgParser

WEBSITE_NAME = "swiggy"
SWIGGY_INCLUDE_KEYS = ["unit", "price", "mrp", "in_stock", "location"]
SWIGGY_KEY_COLUMN_MAP = {
    "unit": "unit size_instamart",
    "price": "price_instamart",
    "mrp": "mrp_instamart",
    "in_stock": "instock_instamart",
    "location": "location_instamart",
}


class SwiggyScrapeBatcher:
    def __init__(self, skip_exists: bool = True, date_str: str = None):
        self.skip_exists = skip_exists
        self.excel_reader = ExcelReader()
        self.switcher = SwiggyLocationSwitcher()
        self.scraper = SwiggyBrowserScraper(date_str=date_str)
        self.extractor = SwiggyProductDataExtractor()
        self.addr_extractor = LocalAddressExtractor(website_name=WEBSITE_NAME)
        self.checker = SwiggyLocationChecker()

    def close_switcher(self):
        try:
            self.switcher.client.close_other_tabs(create_new_tab=True)
        except Exception as e:
            logger.warn(f"× SwiggyScrapeBatcher.close_switcher: {e}")

    def close_scraper(self):
        try:
            self.scraper.client.close_other_tabs(create_new_tab=True)
        except Exception as e:
            logger.warn(f"× SwiggyScrapeBatcher.close_scraper: {e}")

    def run(self):
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
                self.checker.check_product_location(
                    product_info, location_idx, extra_msg="SwiggyScrapeBatcher"
                )
                extracted_data = self.extractor.extract(product_info)
                if extracted_data:
                    sleep(3)

        self.close_scraper()


class RefProductDataLoader:
    def __init__(self, date_str: str = None) -> None:
        self.date_str = date_str
        self.blinkit_batcher = BlinkitExtractBatcher(date_str=date_str)
        self.zepto_batcher = ZeptoExtractBatcher(date_str=date_str)

    def get_product_id(self, df: pd.DataFrame, col_name: str, idx: int) -> str:
        product_info_row = df.iloc[idx]
        product_link = product_info_row.get(col_name, "")
        product_id = product_link.split("/")[-1].strip()
        return product_id

    def load(self, location_name: str, idx: int) -> Union[int, float]:
        product_data = {}
        for col, batcher, df in zip(
            ["weblink_blinkit", "weblink_zepto"],
            [self.blinkit_batcher, self.zepto_batcher],
            [self.blinkit_batcher.excel_reader.df, self.zepto_batcher.excel_reader.df],
        ):
            col_name, _, _ = match_val(col, df.columns.to_list(), use_fuzz=True)
            product_id = self.get_product_id(df, col_name=col_name, idx=idx)
            if not product_id:
                continue
            product_info, _ = batcher.load_product_info(
                product_id=product_id, location_name=location_name
            )
            product_data = batcher.extractor.extract(product_info)
            if product_data.get("mrp"):
                break
        mrp = product_data.get("mrp", None)
        return mrp


class SwiggyExtractBatcher:
    def __init__(self, date_str: str = None, verbose: bool = False):
        self.date_str = date_str
        self.verbose = verbose
        self.excel_reader = ExcelReader(verbose=verbose)
        self.extractor = SwiggyProductDataExtractor()
        self.ref_loader = RefProductDataLoader(date_str=date_str)
        self.checker = SwiggyLocationChecker()
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
            output_path = self.output_root / f"{self.date_str}_swiggy_{name}.xlsx"
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
                        product_info, location_idx, extra_msg="SwiggyExtractBatcher"
                    )
                except Exception as e:
                    logger.warn(
                        f"    * swiggy.{location_name}.{product_id}: "
                        f"{logstr.file(brk(product_info_path))}"
                    )
                    raise e
                ref_mrp = self.ref_loader.load(
                    location_name=location_name, idx=link_idx
                )
                extracted_data = self.extractor.extract(product_info, ref_mrp=ref_mrp)
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


def run_scrape_batcher(args: argparse.Namespace):
    try:
        scraper_batcher = SwiggyScrapeBatcher(
            skip_exists=not args.force_scrape, date_str=args.date
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
        extract_batcher = SwiggyExtractBatcher(date_str=args.date)
        extract_batcher.run()


if __name__ == "__main__":
    arg_parser = BatcherArgParser()
    args = arg_parser.parse_args()

    with Runtimer():
        main(args)

    # Case 1: Batch scrape
    # python -m web.swiggy.batcher -s

    # Case 2: Batch extract
    # python -m web.swiggy.batcher -e

    # Case 3: Batch scrape and extract
    # python -m web.swiggy.batcher -s -e
    # python -m web.swiggy.batcher -s -e -f
