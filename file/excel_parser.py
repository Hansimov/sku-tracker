import pandas as pd

from pathlib import Path
from tclogger import logger, match_val

from configs.envs import SKU_XLSX


class ExcelReader:
    def __init__(self, file_path: Path = SKU_XLSX):
        self.file_path = file_path
        self.init_df()

    def init_df(self):
        logger.note("> Reading Excel to DataFrame ...")
        self.df = pd.read_excel(self.file_path, header=0, engine="openpyxl")
        self.columns = self.df.columns.tolist()
        logger.file(f"  * {self.file_path}")

    def get_column_by_name(self, column: str) -> pd.Series:
        _, column_idx, _ = match_val(column, self.columns, use_fuzz=True)
        if column_idx is None:
            return None
        else:
            return self.df[self.columns[column_idx]]


if __name__ == "__main__":
    reader = ExcelReader(SKU_XLSX)
    print(reader.df)
    print(reader.get_column_by_name("weblink_blinkit"))

    # python -m file.excel_parser
