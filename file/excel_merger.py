import argparse
import pandas as pd
import openpyxl
import sys
import warnings

from openpyxl.worksheet.worksheet import Worksheet
from pathlib import Path
from tclogger import logger, logstr, brk, match_val
from tclogger import get_now, get_now_str, str_to_t
from typing import Union, Literal
from datetime import datetime, timedelta

from configs.envs import DATA_ROOT, LOCATION_LIST, LOCATION_MAP, WEBSITE_NAMES

warnings.filterwarnings("ignore", category=FutureWarning)

DISCOUNT_COLUMNS_MAP = {
    "blinkit": {
        "disc": "Disc_Blinkit",
        "price": "price_blinkit",
        "mrp": "mrp_blinkit",
    },
    "swiggy": {
        "disc": "Disc_Instamart",
        "price": "price_instamart",
        "mrp": "mrp_instamart",
    },
    "zepto": {
        "disc": "Disc_Zepto",
        "price": "price_zepto",
        "mrp": "mrp_zepto",
    },
    "zepto_supersaver": {
        "disc": "Disc_ZeptoSuperSaver",
        "price": "price_supersaver_zepto",
        "mrp": "mrp_zepto",
    },
    "dmart": {
        "disc": "Disc_Dmart",
        "price": "price_dmart",
        "mrp": "mrp_dmart",
    },
}
EXCLUDE_COLUMNS = [
    "location_blinkit",
    "location_zepto",
    "location_instamart",
    "location_dmart",
]


def get_location_val(location: str) -> str:
    return LOCATION_MAP.get(location, location)


def merge_dfs_horizontally(dfs: list[pd.DataFrame]) -> pd.DataFrame:
    """Merge all dfs into one df: concatenate columns"""
    if not dfs:
        return pd.DataFrame()
    merged_df = dfs[0].copy()
    for df in dfs[1:]:
        for col in df.columns:
            if col not in merged_df.columns:
                merged_df[col] = df[col]
            else:
                # update each row with non-nan value
                mask = merged_df[col].isna() | merged_df[col].eq("")
                merged_df.loc[mask, col] = df.loc[mask, col]
    return merged_df


def merge_dfs_vertically(dfs: list[pd.DataFrame]) -> pd.DataFrame:
    """Merge all dfs into one df: concatenate rows"""
    if not dfs:
        return pd.DataFrame()
    merged_df = pd.concat(dfs, ignore_index=True)
    return merged_df


def merge_dfs(
    dfs: list[pd.DataFrame],
    direction: Literal["horizontal", "vertical"] = "horizontal",
) -> pd.DataFrame:
    if direction.lower().startswith("h"):
        return merge_dfs_horizontally(dfs)
    elif direction.lower().startswith("v"):
        return merge_dfs_vertically(dfs)
    else:
        raise ValueError(f"Invalid merge direction: {direction}")


class DataframeEditor:
    def remove_columns(
        self,
        df: pd.DataFrame,
        columns: list[str] = EXCLUDE_COLUMNS,
        inplace: bool = True,
    ) -> pd.DataFrame:
        logger.note(f"> Removing columns: {logstr.file(columns)}")
        if not inplace:
            df = df.copy()
        for col in columns:
            col_name, _, _ = match_val(col, df.columns.tolist(), use_fuzz=True)
            if col_name in df.columns:
                df.drop(columns=col_name, inplace=True)
        return df

    def insert_date_and_location_columns(
        self, df: pd.DataFrame, location_name: str, date_str: str
    ) -> pd.DataFrame:
        logger.note(f"> Inserting date and location columns ...")
        if "date" not in df.columns:
            date_val = date_str.replace("-", "/")
            df.insert(0, "Date", date_val)
        if "location" not in df.columns:
            location_val = get_location_val(location_name)
            df.insert(1, "Location", location_val)
        return df

    def check_price(self, price: Union[float, int]) -> Union[bool, float]:
        if not price:
            return False
        if isinstance(price, str):
            try:
                price = float(price)
            except Exception as e:
                raise e
        if isinstance(price, (float, int)):
            if price <= 0:
                return False
            else:
                return price
        else:
            raise ValueError(f"invalid price type: {price}")

    def insert_discount_columns(
        self, df: pd.DataFrame, val_format: Literal["float", "percent"] = "float"
    ) -> pd.DataFrame:
        """discount = (1 - price/mrp) * 100 %"""
        logger.note(f"> Inserting discount columns ...")
        for _, col_map in DISCOUNT_COLUMNS_MAP.items():
            columns = df.columns.tolist()
            discount_col = col_map["disc"]

            price_col_name, price_col_idx, _ = match_val(
                col_map["price"], columns, use_fuzz=True
            )
            mrp_col_name, mrp_col_idx, _ = match_val(
                col_map["mrp"], columns, use_fuzz=True
            )

            if price_col_name is None or mrp_col_name is None:
                continue

            discount_values = []
            for _, row in df.iterrows():
                price = row[price_col_name]
                mrp = row[mrp_col_name]
                if val_format == "percent":
                    discount_val_default = ""
                else:
                    discount_val_default = ""
                try:
                    price_num = self.check_price(price)
                    mrp_num = self.check_price(mrp)
                    if price_num is False or mrp_num is False:
                        discount_values.append(discount_val_default)
                        continue
                    discount = 1 - price_num / mrp_num
                    if val_format == "percent":
                        discount_val = f"{discount*100:.0f}%"
                    else:
                        discount_val = round(discount, 2)
                    discount_values.append(discount_val)
                except Exception as e:
                    discount_values.append(discount_val_default)
                    logger.warn(f"× Cannot calc discount: {e}")

            discount_col_idx = price_col_idx + 1
            df.insert(discount_col_idx, discount_col, discount_values)
        return df


class ExcelMerger:
    def __init__(self, date_str: str = None):
        self.date_str = date_str or get_now_str()[:10]
        self.editor = DataframeEditor()
        self.init_paths()
        self.init_workbook()

    def init_paths(self):
        self.output_root = DATA_ROOT / "output" / self.date_str
        self.output_merge_path = self.output_root / f"sku_{self.date_str}.xlsx"

    def init_workbook(self):
        self.workbook = openpyxl.Workbook()
        # Remove default sheet created by openpyxl
        if "Sheet" in self.workbook.sheetnames:
            self.workbook.remove(self.workbook["Sheet"])

    def get_xlsx_paths_by_location(self, location_name: str) -> list[Path]:
        """Extract xlsx files end with same location_name for each website"""
        xlsx_paths = []
        for website in WEBSITE_NAMES:
            output_dir_by_website = self.output_root / website
            if not output_dir_by_website.exists():
                logger.warn(f"  × No output for website: {brk(website)}")
                continue
            for xlsx_path in output_dir_by_website.glob(f"*_{location_name}.xlsx"):
                if xlsx_path.is_file():
                    xlsx_paths.append(xlsx_path)
        return xlsx_paths

    def read_df_list_from_xlsx_files_with_same_location(
        self, location_name: str
    ) -> list[pd.DataFrame]:
        """Read all xlsx files with same location name, and return a list of DataFrames"""
        logger.note(
            f"> Reading xlsx files for location: {logstr.mesg(brk(location_name))}"
        )
        xlsx_paths = self.get_xlsx_paths_by_location(location_name)
        if not xlsx_paths:
            raise FileNotFoundError(
                f"No xlsx files found for location: {location_name}"
            )
        df_list = []
        for xlsx_path in xlsx_paths:
            df = pd.read_excel(
                xlsx_path, header=0, engine="openpyxl", keep_default_na=False
            )
            df_list.append(df)
        return df_list

    def set_sheet_styles(self, sheet: Worksheet):
        logger.note(f"> Setting sheet styles ...")
        # set discount columns number_format to percentage
        for _, col_map in DISCOUNT_COLUMNS_MAP.items():
            discount_col = col_map["disc"]
            # get discount column idx in header
            for col_idx, cell in enumerate(sheet[1], 1):
                if cell.value == discount_col:
                    # apply to entire column
                    for row in range(2, sheet.max_row + 1):
                        sheet.cell(row=row, column=col_idx).number_format = "0%"
                    break

    def write_df_to_sheet(self, df: pd.DataFrame, location_name: str):
        """Write dataframe to new sheet in workbook"""
        sheet_name = f"{self.date_str}_{get_location_val(location_name)}"
        sheet = self.workbook.create_sheet(title=sheet_name)
        # write headers
        for col_idx, column in enumerate(df.columns, 1):
            sheet.cell(row=1, column=col_idx, value=column)

        # write data
        for row_idx, (_, row) in enumerate(df.iterrows(), 2):
            for col_idx, value in enumerate(row, 1):
                sheet.cell(row=row_idx, column=col_idx, value=value)
        self.set_sheet_styles(sheet)

    def merge(self):
        logger.note(f"> Merging xlsx files for:")
        logger.mesg(f"  * locations: {logstr.file(LOCATION_LIST)}")
        logger.mesg(f"  * websites : {logstr.file(WEBSITE_NAMES)}")
        for location_name in LOCATION_LIST:
            df_list = self.read_df_list_from_xlsx_files_with_same_location(
                location_name
            )
            merged_df = merge_dfs(df_list, "horizontal")
            merged_df = self.editor.insert_discount_columns(merged_df)
            merged_df = self.editor.insert_date_and_location_columns(
                merged_df, location_name, self.date_str
            )
            merged_df = self.editor.remove_columns(merged_df)
            print(merged_df)
            self.write_df_to_sheet(merged_df, location_name)

        logger.note(f"> Save merged xlsx to:")
        self.workbook.save(self.output_merge_path)
        logger.okay(f"  * {self.output_merge_path}")


class ExcelPackager:
    """Pack multiple Excel files (per week) from ExcelMerger (per day) into one file."""

    def __init__(self, date_str: str = None, past_days: int = 7):
        self.date_str = date_str or get_now_str()[:10]
        self.past_days = past_days
        self.init_dates()
        self.init_package_path()
        self.init_xlsx_paths()
        self.init_workbook()

    def init_dates(self) -> list[str]:
        """Get past N days by date_str"""
        start_date = str_to_t(self.date_str)
        self.dates: list[datetime] = []
        self.date_strs: list[str] = []
        for i in range(self.past_days, 0, -1):
            date = start_date - timedelta(days=i - 1)
            date_str = date.strftime("%Y-%m-%d")
            self.dates.append(date)
            self.date_strs.append(date_str)
        return self.date_strs

    def init_package_path(self):
        self.output_root = DATA_ROOT / "output"
        self.package_root = DATA_ROOT / "package"
        date_str_beg = self.date_strs[0].replace("-", "")
        date_str_end = self.date_strs[-1].replace("-", "")
        date_mark = f"{date_str_beg}_{date_str_end}"
        date_week = self.dates[0].isocalendar().week
        self.package_path = self.package_root / f"sku_ww{date_week}_{date_mark}.xlsx"

    def init_xlsx_paths(self) -> list[Path]:
        """Get all xlsx paths by date_strs"""
        self.xlsx_paths = []
        for date_str in self.date_strs:
            xlsx_path = self.output_root / f"{date_str}" / f"sku_{date_str}.xlsx"
            self.xlsx_paths.append(xlsx_path)
        return self.xlsx_paths

    def init_workbook(self):
        self.workbook = openpyxl.Workbook()
        if "Sheet" in self.workbook.sheetnames:
            self.workbook.remove(self.workbook["Sheet"])

    def read_df_from_xlsx(self, xlsx_path: Path) -> pd.DataFrame:
        """Read all sheets from xlsx file and merge them into one DataFrame"""
        xlsx = pd.ExcelFile(xlsx_path, engine="openpyxl")
        df_list = []
        for sheet_name in xlsx.sheet_names:
            df = pd.read_excel(xlsx_path, sheet_name=sheet_name, engine="openpyxl")
            df_list.append(df)
        merge_df = merge_dfs(df_list, "vertical")
        print(merge_df.tail())
        return merge_df

    def write_df_to_sheet(self, df: pd.DataFrame, date_str: str):
        """Write dataframe to new sheet in workbook"""
        sheet_name = f"{date_str}"
        sheet = self.workbook.create_sheet(title=sheet_name)
        # write headers
        for col_idx, column in enumerate(df.columns, 1):
            sheet.cell(row=1, column=col_idx, value=column)

        # write data
        for row_idx, (_, row) in enumerate(df.iterrows(), 2):
            for col_idx, value in enumerate(row, 1):
                sheet.cell(row=row_idx, column=col_idx, value=value)

    def save_workbook(self):
        logger.note(f"> Save packaged xlsx to:")
        if not self.package_path.parent.exists():
            self.package_path.parent.mkdir(parents=True, exist_ok=True)
        self.workbook.save(self.package_path)
        logger.okay(f"  * {self.package_path}")

    def package(self):
        logger.note(f"> Packaging xlsx files for:")
        for date_str, xlsx_path in zip(self.date_strs, self.xlsx_paths):
            if not xlsx_path.exists():
                logger.warn(f"  × {xlsx_path}")
                continue
            logger.file(f"  * {xlsx_path}")
            df = self.read_df_from_xlsx(xlsx_path)
            self.write_df_to_sheet(df, date_str)
        self.save_workbook()


class ExcelMergerArgParser(argparse.ArgumentParser):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.add_argument("-d", "--date", type=str, default=None)
        self.add_argument("-m", "--merge", action="store_true")
        self.add_argument("-p", "--package", action="store_true")

    def parse_args(self):
        self.args, self.unknown_args = self.parse_known_args(sys.argv[1:])
        return self.args


def main(args: argparse.Namespace):
    if args.merge:
        merger = ExcelMerger(date_str=args.date)
        merger.merge()

    if args.package:
        packager = ExcelPackager(date_str=args.date)
        packager.package()


if __name__ == "__main__":
    arg_parser = ExcelMergerArgParser()
    args = arg_parser.parse_args()

    main(args)

    # Case 1: Extract data from websites and save to Excel files
    # python -m web.blinkit.batcher -e
    # python -m web.zepto.batcher -e
    # python -m web.swiggy.batcher -e

    # Case 2: Merge Excel files (daily) into one
    # python -m file.excel_merger -m

    # Case 3: Package Excel files (weekly) into one
    # python -m file.excel_merger -p
