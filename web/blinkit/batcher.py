import argparse
import json
import sys

from copy import deepcopy
from tclogger import logger, logstr, brk, Runtimer, TCLogbar, TCLogbarGroup
from tclogger import dict_to_str, dict_get, get_now_str
from time import sleep
from pathlib import Path
from typing import Union
from urllib.parse import unquote

from configs.envs import DATA_ROOT, LOCATIONS, SKU_XLSX
from file.excel_parser import ExcelReader, DataframeParser
from web.blinkit.scraper import BlinkitBrowserScraper, BlinkitProductDataExtractor

BLINKIT_INCLUDE_KEYS = ["unit", "price", "mrp", "in_stock"]
BLINKIT_KEY_COLUMN_MAP = {
    "unit": "unit size_blinkit",
    "price": "price_blinkit",
    "mrp": "mrp_blinkit",
    "in_stock": "instock_blinkit",
}


class BlinkitScrapeBatcher:
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
                        f"[{logstr.mesg(link_idx+1)}/{logstr.file(links_count)}]",
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
                if extracted_data:
                    sleep(1)


class BlinkitExtractBatcher:
    def __init__(self, verbose: bool = False):
        self.excel_reader = ExcelReader(verbose=verbose)
        self.extractor = BlinkitProductDataExtractor()
        self.verbose = verbose
        self.init_paths()

    def init_paths(self):
        date_str = get_now_str()[:10]
        self.dump_root = DATA_ROOT / "dumps" / date_str / "blinkit"
        self.output_root = DATA_ROOT / "output" / date_str / "blinkit"
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
            output_path = self.output_root / f"{self.date_str}_blinkit_{name}.xlsx"
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

    def check_location(self, product_info: dict, location_idx: int):
        location_dict = LOCATIONS[location_idx]
        dump_locality = dict_get(product_info, ["cookies", "gr_1_locality"], "")
        dump_landmark = dict_get(product_info, ["cookies", "gr_1_landmark"], "")
        correct_locality = location_dict.get("locality", "")
        if unquote(dump_locality.lower()) != unquote(correct_locality.lower()):
            err_mesg = f"  × Location dumpped incorrectly!"
            logger.warn(err_mesg)
            info_dict = {
                "dump_locality": dump_locality,
                "dump_landmark": dump_landmark,
                "correct_locality": correct_locality,
            }
            logger.mesg(dict_to_str(info_dict), indent=4)
            raise ValueError(err_mesg)
        return True

    def run(self):
        blinkit_links = self.excel_reader.get_column_by_name("weblink_blinkit")
        location_bar = TCLogbar(total=len(LOCATIONS), head="Location:")
        product_bar = TCLogbar(total=len(blinkit_links), head=" * Product:")
        TCLogbarGroup([location_bar, product_bar])
        for location_idx, location_item in enumerate(LOCATIONS):
            df = deepcopy(self.excel_reader.df)
            df_parser = DataframeParser(df, verbose=self.verbose)
            location_name = location_item.get("name", "")
            links = blinkit_links[:]
            location_bar.update(desc=logstr.mesg(brk(location_name)), flush=True)
            row_dicts: list[dict] = []
            for link_idx, link in enumerate(links):
                product_bar.update(increment=1)
                if not link:
                    logger.mesg(f"* Skip empty link at row [{link_idx}]")
                    row_dicts.append({})
                    continue
                product_id = link.split("/")[-1]
                product_bar.set_desc(logstr.mesg(brk(product_id)))
                product_info = self.load_product_info(
                    product_id=product_id, location_name=location_name
                )
                self.check_location(product_info, location_idx)
                extracted_data = self.extractor.extract(product_info)
                row_dicts.append(extracted_data)
            output_path = self.get_output_path(location_name)
            renamed_row_dicts = df_parser.rename_row_dicts_keys_to_column(
                row_dicts=row_dicts,
                key_column_map=BLINKIT_KEY_COLUMN_MAP,
                include_keys=BLINKIT_INCLUDE_KEYS,
            )
            df_parser.update_df_by_row_dicts(renamed_row_dicts)
            df_parser.dump_to_excel(
                output_path=output_path, sheet_name=output_path.stem
            )
            location_bar.update(increment=1, flush=True)
        print()


class BlinkitBatcherArgParser(argparse.ArgumentParser):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.add_argument("-s", "--scrape", action="store_true")
        self.add_argument("-e", "--extract", action="store_true")

    def parse_args(self):
        self.args, self.unknown_args = self.parse_known_args(sys.argv[1:])
        return self.args


def main(args: argparse.Namespace):
    if args.scrape:
        scraper_batcher = BlinkitScrapeBatcher()
        scraper_batcher.run()

    if args.extract:
        extract_batcher = BlinkitExtractBatcher()
        extract_batcher.run()

    if not (args.scrape or args.extract):
        logger.warn("No valid argument: `-s` for scrape or `-e` for extract.")


if __name__ == "__main__":
    arg_parser = BlinkitBatcherArgParser()
    args = arg_parser.parse_args()
    with Runtimer():
        main(args)

    # Case 1: Batch scrape
    # python -m web.blinkit.batcher -s

    # Case 2: Batch extract
    # python -m web.blinkit.batcher -e
