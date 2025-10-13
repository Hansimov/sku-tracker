import pandas as pd

from tclogger import logger, match_val
from typing import Union

from web.blinkit.batcher import BlinkitExtractBatcher
from web.zepto.batcher import ZeptoExtractBatcher


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

    def load(self, location_name: str, idx: int, key: str = "mrp") -> Union[int, float]:
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
            try:
                product_info, _ = batcher.load_product_info(
                    product_id=product_id, location_name=location_name
                )
                product_data = batcher.extractor.extract(product_info)
                if product_data.get(key):
                    break
            except Exception as e:
                logger.warn(e)
                continue
        mrp = product_data.get(key, None)
        return mrp
