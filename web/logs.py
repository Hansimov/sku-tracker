import pandas as pd

from tclogger import logger, logstr


def log_link_idx(link_idx: int, total_links: int):
    logger.note(
        f"[{logstr.mesg(link_idx + 1)}/{logstr.file(total_links)}]",
        end=" ",
    )


def log_df_tail(df: pd.DataFrame, n: int = 5):
    logger.mesg(f"> DataFrame tail {n} rows:")
    with pd.option_context("display.show_dimensions", False):
        logger.line(df.tail(n), indent=2)


def log_df_dims(df: pd.DataFrame):
    row_cnt, col_cnt = df.shape
    logger.mesg(f"* [{logstr.file(row_cnt)} rows x {logstr.file(col_cnt)} cols]")
