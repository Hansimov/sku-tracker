import pandas as pd
import warnings

from pathlib import Path
from tclogger import logger, match_val

from configs.envs import SKU_XLSX

warnings.filterwarnings("ignore", category=FutureWarning)


class ExcelReader:
    def __init__(self, file_path: Path = SKU_XLSX, verbose: bool = True):
        self.file_path = file_path
        self.verbose = verbose
        self.init_df()

    def init_df(self):
        logger.enter_quiet(not self.verbose)
        logger.note("> Reading DataFrame from Excel:")
        self.df = pd.read_excel(self.file_path, header=0, engine="openpyxl")
        self.columns = self.df.columns.tolist()
        logger.file(f"  * {self.file_path}")
        logger.exit_quiet(not self.verbose)

    def get_column_by_name(self, column: str) -> pd.Series:
        _, column_idx, _ = match_val(column, self.columns, use_fuzz=True)
        if column_idx is None:
            return None
        else:
            return self.df[self.columns[column_idx]]


class DataframeParser:
    def __init__(self, df: pd.DataFrame, verbose: bool = True):
        self.df = df
        self.verbose = verbose

    def rename_row_dicts_keys_to_column(
        self,
        row_dicts: list[dict],
        key_column_map: dict = {},
        include_keys: list[str] = None,
    ) -> list[dict]:
        if not row_dicts or not key_column_map:
            return row_dicts
        renamed_row_dicts = []
        for row_dict in row_dicts:
            renamed_row = {}
            for key, val in row_dict.items():
                if include_keys is not None and key not in include_keys:
                    continue
                if key in key_column_map:
                    new_key = match_val(
                        key_column_map[key], self.df.columns.to_list(), use_fuzz=True
                    )[0]
                    renamed_row[new_key] = val
                else:
                    renamed_row[key] = val
            renamed_row_dicts.append(renamed_row)
        return renamed_row_dicts

    def update_df_by_row_dicts(self, row_dicts: list[dict]):
        if len(row_dicts) != len(self.df):
            logger.warn(
                f"× Inequal length of row_dicts({len(row_dicts)}) vs df({len(self.df)})"
            )
            return

        for idx, row_dict in enumerate(row_dicts):
            for key, val in row_dict.items():
                if key in self.df.columns:
                    self.df.at[idx, key] = val
                else:
                    logger.warn(f"× Invalid column: '{key}'")
                    continue

    def dump_to_excel(self, output_path: Path = None, sheet_name: str = "Sheet1"):
        logger.enter_quiet(not self.verbose)
        logger.note(f"> Dumping DataFrame to Excel:")
        output_path.parent.mkdir(parents=True, exist_ok=True)
        self.df.to_excel(
            output_path, sheet_name=sheet_name, index=False, engine="openpyxl"
        )
        logger.file(f"  * {output_path}")
        logger.exit_quiet(not self.verbose)


if __name__ == "__main__":
    reader = ExcelReader(SKU_XLSX)
    print(reader.df)
    print(reader.get_column_by_name("weblink_blinkit"))

    # python -m file.excel_parser
