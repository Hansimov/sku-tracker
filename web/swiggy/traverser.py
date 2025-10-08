import argparse
import json
import pandas as pd
import re

from dataclasses import dataclass
from DrissionPage._pages.chromium_tab import ChromiumTab
from pathlib import Path
from time import sleep, monotonic
from tclogger import logger, logstr, brk, get_now_str, Runtimer, dict_get
from typing import Literal
from urllib.parse import parse_qs, urlparse

from configs.envs import DATA_ROOT, SWIGGY_LOCATIONS, SWIGGY_TRAVERSER_SETTING
from web.swiggy.scraper import SwiggyLocationChecker, SwiggyLocationSwitcher
from web.blinkit.traverser import norm_name, load_json, raise_breakpoint
from web.browser import BrowserClient
from web.constants import norm_date_str
from cli.arg import TraverserArgParser


WEBSITE_NAME = "swiggy"
SWIGGY_CATEG_URL = "https://www.swiggy.com/instamart"
SWIGGY_API_HOME_URL = "https://www.swiggy.com/api/instamart/home/v2\?.*"


def get_dump_root(date_str: str = None) -> Path:
    return DATA_ROOT / "traverses" / norm_date_str(date_str) / WEBSITE_NAME


def get_summary_root(date_str: str = None) -> Path:
    return DATA_ROOT / "summaries" / norm_date_str(date_str) / WEBSITE_NAME


def get_categ_dump_path(date_str: str = None, location: str = None) -> Path:
    dump_root = get_dump_root(date_str)
    path_parts = ["categories.json"]
    if location:
        path_parts = [location] + path_parts
    return dump_root.joinpath(*path_parts)


class SwiggyCategoriesExtractor:
    def __init__(self, client: BrowserClient, verbose: bool = False):
        self.client = client
        self.verbose = verbose

    def extract(self, categ_json: dict) -> dict:
        logger.enter_quiet(not self.verbose)
        if not categ_json:
            logger.warn("  × Empty response json to extract")
            logger.exit_quiet(not self.verbose)
            return {}
        categ_data = {}
        categories = dict_get(categ_json, "data.cards", [])
        if categories:
            categ_data = {"categories": categories, "count": len(categories)}
        else:
            categ_data = {"categories": [], "count": -1}
        logger.exit_quiet(not self.verbose)
        return categ_data


class SwiggyCategoriesFetcher:
    def __init__(
        self, client: BrowserClient, date_str: str = None, location: str = None
    ):
        self.client = client
        self.date_str = norm_date_str(date_str)
        self.extractor = SwiggyCategoriesExtractor(client=client, verbose=True)
        self.location = location

    def get_cookies(self, tab: ChromiumTab) -> dict:
        cookies_dict = tab.cookies(all_info=True).as_dict()
        cookies_dict["url"] = tab.url
        cookies_dict["now"] = get_now_str()
        return cookies_dict

    def fetch(self) -> dict:
        categ_url = SWIGGY_CATEG_URL
        logger.note(f"> Visiting categories url:")
        logger.file(f"  * {categ_url}")

        self.client.start_client()
        tab = self.client.browser.latest_tab
        tab.set.load_mode.none()

        listen_targets = [SWIGGY_API_HOME_URL]
        tab.listen.start(targets=listen_targets, is_regex=True)
        tab.get(categ_url)
        logger.mesg(f"  ✓ Title: {brk(tab.title)}")

        logger.note(f"  > Listening targets:")
        for target in listen_targets:
            logger.file(f"    * {target}")

        categ_packet = None
        categ_data = {}
        for packet in tab.listen.steps(timeout=30):
            packet_url = packet.url
            packet_url_str = logstr.file(brk(packet_url))
            if re.match(SWIGGY_API_HOME_URL, packet_url):
                logger.okay(f"  + Categories packet captured: {packet_url_str}")
                tab.stop_loading()
                categ_packet = packet
                break
            else:
                logger.warn(f"  × Unexpected packet: {packet_url_str}")

        if categ_packet:
            categ_resp = categ_packet.response
            if categ_resp:
                categ_json = categ_resp.body
                categ_data = self.extractor.extract(categ_json)

        if categ_data:
            categ_data["cookies"] = self.get_cookies(tab)

        self.client.stop_client(close_browser=False)
        return categ_data

    def dump(self, resp: dict):
        self.dump_path = get_categ_dump_path(self.date_str, location=self.location)
        logger.note(f"  > Dump categories data to json:", end=" ")
        self.dump_path.parent.mkdir(parents=True, exist_ok=True)
        with open(self.dump_path, "w", encoding="utf-8") as wf:
            json.dump(resp, wf, indent=4, ensure_ascii=False)
        logger.okay(f"{brk(self.dump_path)}")

    def run(self):
        resp_data = self.fetch()
        if resp_data:
            if resp_data:
                self.dump(resp_data)
        else:
            return {}


class SwiggyListingExtractor:
    def snippet_to_dict(self, snippet: dict) -> dict:
        data = dict_get(snippet, "data", {})
        atc = dict_get(data, "atc_action.add_to_cart.cart_item", {})
        snippet_dict = {
            "product_id": dict_get(atc, "product_id", None),
            "product_name": dict_get(atc, "product_name", None),
            "quantity": dict_get(atc, "quantity", None),
            "price": dict_get(atc, "price", None),
            "mrp": dict_get(atc, "mrp", None),
            "unit": dict_get(atc, "unit", None),
            "inventory": dict_get(atc, "inventory", None),
            "group_id": dict_get(atc, "group_id", None),
            "brand": dict_get(atc, "brand", None),
            "is_sold_out": dict_get(data, "is_sold_out", None),
            "product_state": dict_get(data, "product_state", None),
        }
        return snippet_dict

    def extract(self, resp: dict) -> list[dict]:
        res = []
        snippets = dict_get(resp, "response.snippets") or []
        for snippet in snippets:
            snippet_dict = self.snippet_to_dict(snippet)
            res.append(snippet_dict)
        return res


class SwiggyListingScroller:
    def __init__(self):
        self.scroll_js = """
        (async () => {
            const container = document.querySelector('#plpContainer');
            if (!container) {
                return { ok: false, reason: 'container-not-found' };
            }
            // scroll up
            const topBeforeUp = container.scrollTop;
            container.scrollTop = Math.max(topBeforeUp - 100, 0);
            // sleep to ensure scroll up completed
            await new Promise(resolve => setTimeout(resolve, 1000));
            // scroll down
            const topBeforeDown = container.scrollTop;
            const maxScroll = Math.max(container.scrollHeight - container.clientHeight, 0);
            container.scrollTop = maxScroll;
            return { ok: true };
        })()
        """

    def scroll(self, tab: ChromiumTab) -> bool:
        try:
            logger.note(f"  > Scrolling listing container ...")
            js_res = tab.run_js(self.scroll_js, as_expr=True)
        except Exception as e:
            logger.warn(f"  × Scroll JS failed: {e}")
            raise e
            # return False
        if isinstance(js_res, dict) and js_res.get("ok"):
            return True
        else:
            logger.warn(f"  × Scroll JS result: {js_res}")
            return False


@dataclass
class SwiggySubCategoryContext:
    sidx: int
    stotal: int
    sname: str
    sid: int
    url: str
    json_path: Path
    cname: str
    cid: int
    data: dict

    def idx_str(self) -> str:
        return f"[{logstr.file(self.sidx)}/{logstr.mesg(self.stotal)}]"

    def label_str(self) -> str:
        return logstr.note(f"{self.sname} (cid/{self.cid}/{self.sid})")

    def idx_label_str(self) -> str:
        return f"{self.idx_str()} {self.label_str()}"

    def log_info(self):
        logger.note(f"  * {self.idx_label_str()}:", end=" ")
        logger.file(f"{self.url}")


@dataclass
class SwiggyCategoryContext:
    cidx: int
    ctotal: int
    cname: str
    cid: int
    data: dict
    sctxs: list[SwiggySubCategoryContext]

    def idx_str(self) -> str:
        return f"[{logstr.file(self.cidx)}/{logstr.mesg(self.ctotal)}]"

    def label_str(self) -> str:
        return logstr.note(f"{self.cname} (cid/{self.cid})")

    def idx_label_str(self) -> str:
        return f"{self.idx_str()} {self.label_str()}"

    def log_info(self):
        logger.note(f"> {self.idx_label_str()}")


class SwiggyCategoryIterator:
    def __init__(self, date_str: str = None, location: str = None):
        self.date_str = norm_date_str(date_str)
        self.location = location
        self.dump_root = get_dump_root(self.date_str)
        self.load_categories()

    def load_categories(self) -> list[dict]:
        self.categ_path = get_categ_dump_path(
            date_str=self.date_str, location=self.location
        )
        if not self.categ_path.exists():
            return []
        with open(self.categ_path, "r", encoding="utf-8") as rf:
            categ_data = json.load(rf)
        self.categories = categ_data.get("categories", []) or []

    def get_json_path(self, cid: int, sid: int) -> Path:
        cid_str = str(cid)
        parts = [cid_str, f"{cid_str}_{sid}.json"]
        if self.location:
            parts = [self.location] + parts
        return self.dump_root.joinpath(*parts)

    def get_sub_categ_url(self, name: str, cid: int, sid: int) -> str:
        mark = norm_name(name)
        return f"https://blinkit.com/cn/{mark}/cid/{cid}/{sid}"

    def __iter__(self):
        ctotal = len(self.categories)
        for cidx, categ in enumerate(self.categories, start=1):
            cname = categ.get("name", "")
            cid = categ.get("id", -1)
            sub_categs = categ.get("subCategories", []) or []
            stotal = len(sub_categs)
            sctxs: list[SwiggySubCategoryContext] = []
            for sidx, sub_categ in enumerate(sub_categs, start=1):
                sname = sub_categ.get("name", "")
                sid = sub_categ.get("id", -1)
                url = self.get_sub_categ_url(sname, cid, sid)
                json_path = self.get_json_path(cid=cid, sid=sid)
                sctxs.append(
                    SwiggySubCategoryContext(
                        sidx=sidx,
                        stotal=stotal,
                        sname=sname,
                        sid=sid,
                        url=url,
                        json_path=json_path,
                        cname=cname,
                        cid=cid,
                        data=sub_categ,
                    )
                )
            yield SwiggyCategoryContext(
                cidx=cidx,
                ctotal=ctotal,
                cname=cname,
                cid=cid,
                data=categ,
                sctxs=sctxs,
            )


class SwiggyCategoryScraper:
    LISTEN_INITIAL_TIMEOUT = 30
    LISTEN_POLL_INTERVAL = 0.5
    LISTEN_DRAIN_TIMEOUT = 0.5

    def __init__(
        self,
        client: BrowserClient,
        switcher: SwiggyLocationSwitcher,
        date_str: str = None,
        location: str = None,
    ):
        self.client = client
        self.switcher = switcher
        self.date_str = norm_date_str(date_str)
        self.location = location
        self.extractor = SwiggyListingExtractor()
        self.scroller = SwiggyListingScroller()
        self.last_offset: int = None
        self.same_offset_count: int = 0

    def reset_offset_state(self):
        self.last_offset = None
        self.same_offset_count = 0

    def extract_offset(self, packet_url: str) -> int:
        if not packet_url:
            return None
        try:
            query = urlparse(packet_url).query
            if not query:
                return None
            offsets = parse_qs(query).get("offset")
            if not offsets:
                return None
            offset_str = offsets[0]
            if offset_str in {None, ""}:
                return None
            return int(offset_str)
        except Exception as e:
            logger.warn(f"  × Failed to parse offset: {e}")
            return None

    def is_listing_end(self, item_count: int, packet_url: str) -> bool:
        if item_count < 15:
            self.reset_offset_state()
            return True

        offset = self.extract_offset(packet_url)
        if offset is None:
            self.reset_offset_state()
            return False

        if offset == self.last_offset:
            self.same_offset_count += 1
        else:
            self.last_offset = offset
            self.same_offset_count = 1

        if self.same_offset_count >= 3:
            self.reset_offset_state()
            return True

        return False

    def collect_packets(self, tab: ChromiumTab, initial_wait: float) -> list:
        packets: list = []
        deadline = monotonic() + initial_wait
        while True:
            remaining = deadline - monotonic()
            if remaining <= 0:
                break
            current_timeout = min(self.LISTEN_POLL_INTERVAL, remaining)
            found_packet = False
            for packet in tab.listen.steps(timeout=current_timeout):
                packets.append(packet)
                found_packet = True
            if found_packet:
                deadline = monotonic() + self.LISTEN_DRAIN_TIMEOUT
            elif packets:
                break
        return packets

    def scrape(self, url: str) -> list:
        tab = self.client.browser.latest_tab
        listen_targets = [...]
        tab.listen.start(targets=listen_targets)
        tab.set.load_mode.none()
        tab.get(url)

        logger.mesg(f"  ✓ Title: {brk(tab.title)}")
        logger.note(f"  > Listening targets:")
        for target in listen_targets:
            logger.file(f"    * {target}")

        products_data = []
        last_action = "navigate"
        self.reset_offset_state()

        while True:
            initial_wait = (
                self.LISTEN_INITIAL_TIMEOUT
                if last_action in {"navigate", "scroll"}
                else self.LISTEN_DRAIN_TIMEOUT
            )
            packets = self.collect_packets(tab, initial_wait)
            if not packets:
                break

            listing_packets_found = False
            should_scroll = False
            for packet in packets:
                packet_url: str = packet.url
                if len(packet_url) >= 70:
                    packet_url_parts = packet_url.split("&")
                    packet_url_str = "&".join(packet_url_parts[:2])
                else:
                    packet_url_str = packet_url
                packet_url_str = logstr.file(brk(packet_url_str))

                if packet_url.startswith(...):
                    listing_packets_found = True
                    logger.okay(
                        f"  + Listing packet captured: {packet_url_str}", end=" "
                    )
                    resp = packet.response
                    if resp:
                        resp_data = self.extractor.extract(resp.body)
                        item_count = len(resp_data)
                        logger.mesg(f"+ Extracted {item_count} items")
                        products_data.extend(resp_data)
                        if self.is_listing_end(
                            item_count=item_count, packet_url=packet_url
                        ):
                            tab.stop_loading()
                            return products_data
                        should_scroll = True
                else:
                    logger.warn(f"  × Unexpected packet: {packet_url_str}")

            if not listing_packets_found:
                last_action = "idle"
                continue

            if not should_scroll:
                break

            scroll_res = self.scroller.scroll(tab)
            last_action = "scroll"
            if not scroll_res:
                logger.warn("  × Unable to scroll listing container")
                break
            sleep(3)

        return products_data

    def skip_json(self, json_path: Path):
        with logger.temp_indent(2):
            logger.mesg(f"  ✓ Skip existed json: {logstr.file(brk(json_path))}")

    def save_json(self, data: list[dict], save_path: Path):
        items = dict_get(data, "products", [])
        logger.okay(f"  ✓ Save {len(items)} items to:", end=" ")
        save_path.parent.mkdir(parents=True, exist_ok=True)
        with open(save_path, "w", encoding="utf-8") as wf:
            json.dump(data, wf, ensure_ascii=False, indent=4)
        logger.okay(f"{brk(save_path)}")

    def check_json_status(
        self, json_path: Path
    ) -> Literal["not_exists", "incomplete", "exists"]:
        if not json_path.exists():
            return "not_exists"
        json_data = load_json(json_path)
        products = dict_get(json_data, "products", []) or []
        items_count = len(products)
        if items_count > 0 and items_count % 15 == 0:
            return "incomplete"
        return "exists"

    def construct_save_data(
        self,
        cctx: SwiggyCategoriesExtractor,
        sctx: SwiggySubCategoryContext,
        products_data: list[dict],
    ) -> dict:
        save_data = {
            "categ": cctx.cname,
            "sub_categ": sctx.sname,
            "url": sctx.url,
            "cid": cctx.cid,
            "sid": sctx.sid,
            "location": self.location,
            "count": len(products_data),
            "products": products_data,
        }
        return save_data

    def process_context(
        self, cctx: SwiggyCategoryContext, sctx: SwiggySubCategoryContext
    ):
        with logger.temp_indent(2):
            products_data = self.scrape(sctx.url)
            save_data = self.construct_save_data(
                cctx=cctx, sctx=sctx, products_data=products_data
            )
            self.save_json(save_data, sctx.json_path)

    def wait_next(self, seconds: int = 8):
        logger.note(f"  > Waiting {seconds}s for next ...")
        sleep(seconds)

    def run(self, location: str = None, location_idx: int = 0):
        self.location = location or self.location
        iterator = SwiggyCategoryIterator(
            date_str=self.date_str, location=self.location
        )
        self.client.start_client()
        for cctx in iterator:
            cctx.log_info()
            for sctx in cctx.sctxs:
                json_path = sctx.json_path
                json_status = self.check_json_status(json_path)
                if json_status == "exists":
                    # self.skip_json(json_path)
                    continue
                if json_status == "incomplete":
                    # logger.warn(f"  ? Items count is 15x, may be incomplete")
                    # raise_breakpoint()
                    continue
                sctx.log_info()
                # not_exists/incomplete: scrape and save
                if not self.switcher.is_at_idx(location_idx):
                    self.switcher.set_location(location_idx)
                self.process_context(cctx=cctx, sctx=sctx)
                self.wait_next(8)
                # raise_breakpoint()
        self.client.stop_client()


class SwiggyTraverser:
    def __init__(
        self,
        skip_exists: bool = True,
        date_str: str = None,
        client_settings: dict = None,
        locations: list = None,
    ):
        self.skip_exists = skip_exists
        self.date_str = norm_date_str(date_str)
        self.client_settings = client_settings or SWIGGY_TRAVERSER_SETTING
        self.locations = locations or SWIGGY_LOCATIONS
        self.client = BrowserClient(**self.client_settings)
        self.checker = SwiggyLocationChecker(locations=self.locations)
        self.switcher = SwiggyLocationSwitcher(
            client_settings=self.client_settings, locations=self.locations
        )
        self.fetcher = SwiggyCategoriesFetcher(
            client=self.client, date_str=self.date_str
        )
        self.scraper = SwiggyCategoryScraper(
            client=self.client, switcher=self.switcher, date_str=self.date_str
        )

    def run(self):
        for location_idx, location_item in enumerate(self.locations[:1]):
            location_name = location_item.get("name", "")
            location_text = location_item.get("text", "")
            self.fetcher.location = location_name
            logger.hint(f"> Location: {location_name} - {location_text}")
            categ_path = get_categ_dump_path(
                date_str=self.date_str, location=location_name
            )
            if self.skip_exists and categ_path.exists():
                logger.mesg(
                    f"> Skip fetch existed categories: {logstr.file(brk(categ_path))}"
                )
            else:
                if not self.switcher.is_at_idx(location_idx):
                    self.switcher.set_location(location_idx)
                self.fetcher.run()
            # self.scraper.run(location=location_name, location_idx=location_idx)


CATEG_COLUMNS = ["categ", "sub_categ", "url", "cid", "sid"]
PRODUCT_COLUMNS = [
    "product_name",
    "brand",
    "product_id",
    "price",
    "mrp",
    "unit",
    "inventory",
    "product_state",
]
DF_INT_COLUMNS = ["cid", "sid", "product_id", "price", "mrp", "inventory"]
COLUMN_RENAMES = {
    "url": "categ_url",
}


class SwiggySummarizer:
    def __init__(self, date_str: str = None, locations: list = None):
        self.date_str = norm_date_str(date_str)
        self.locations = locations or SWIGGY_LOCATIONS
        self.summary_root = get_summary_root(self.date_str)

    def product_dict_to_row(self, product: dict) -> dict:
        if product.get("product_id") is None and product.get("product_name") is None:
            return {}
        row = {col: product.get(col, None) for col in PRODUCT_COLUMNS}
        product_name = row.get("product_name", "")
        product_id = row.get("product_id", None)
        if product_name and product_id:
            product_mark = norm_name(product_name)
            product_link = f"https://blinkit.com/prn/{product_mark}/prid/{product_id}"
        else:
            product_link = None
        row["product_link"] = product_link
        return row

    def categ_dict_to_row(self, data: dict) -> dict:
        return {col: data.get(col, None) for col in CATEG_COLUMNS}

    def get_rows_from_context(
        self, sctx: SwiggySubCategoryContext, location: str
    ) -> list[dict]:
        json_path = sctx.json_path
        if not json_path.exists():
            logger.warn(f"  × JSON not exists: {logstr.file(brk(json_path))}")
            return []
        json_data = load_json(json_path)
        categ_row = self.categ_dict_to_row(json_data)
        products = dict_get(json_data, "products", []) or []
        product_rows = [self.product_dict_to_row(product) for product in products]
        rows = []
        for product_row in product_rows:
            if not product_row:
                continue
            row = {
                "date": self.date_str,
                "location": location,
                **categ_row,
                **product_row,
            }
            rows.append(row)
        return rows

    def rows_to_df(self, rows: list[dict]) -> pd.DataFrame:
        df = pd.DataFrame(rows)
        for col in DF_INT_COLUMNS:
            if col not in df.columns:
                continue
            df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int64")
        if not df.empty:
            df = df.drop_duplicates(ignore_index=True)
        df = df.rename(columns=COLUMN_RENAMES)
        print(df)
        return df

    def get_xlsx_sheet_name(self, location: str) -> tuple[Path, str]:
        return f"{self.date_str}_{WEBSITE_NAME}_{location}"

    def save_df_to_xlsx(self, df: pd.DataFrame, location: str):
        sheet_name = self.get_xlsx_sheet_name(location)
        xlsx_name = f"summary_{sheet_name}.xlsx"
        xlsx_path = self.summary_root / xlsx_name
        logger.note(f"> Save summary to xslx:")
        xlsx_path.parent.mkdir(parents=True, exist_ok=True)
        df.to_excel(xlsx_path, sheet_name=sheet_name, index=False, engine="openpyxl")
        logger.okay(f"  * {brk(xlsx_path)}")

    def save_dfs_to_xlsx(self, df_locs: list[tuple[pd.DataFrame, str]]):
        xlsx_name = f"summary_{self.date_str}_{WEBSITE_NAME}.xlsx"
        xlsx_root = self.summary_root.parent
        xlsx_path = xlsx_root / xlsx_name
        logger.note(f"> Save combined summary to xslx:")
        with pd.ExcelWriter(xlsx_path, engine="openpyxl") as writer:
            for df, location in df_locs:
                sheet_name = self.get_xlsx_sheet_name(location)
                df.to_excel(
                    writer, sheet_name=sheet_name, index=False, engine="openpyxl"
                )
        logger.okay(f"  * {brk(xlsx_path)}")

    def run(self):
        df_locs: list[tuple[pd.DataFrame, str]] = []
        for location_idx, location_item in enumerate(self.locations[:]):
            location = location_item.get("name", "")
            location_text = location_item.get("text", "")
            logger.hint(f"> Location: {location} - {location_text}")
            iterator = SwiggyCategoryIterator(date_str=self.date_str, location=location)
            rows: list[dict] = []
            for cctx in iterator:
                for sctx in cctx.sctxs:
                    sctx_rows = self.get_rows_from_context(sctx=sctx, location=location)
                    rows.extend(sctx_rows)
            df = self.rows_to_df(rows)
            df_locs.append((df, location))

        for df, location in df_locs:
            self.save_df_to_xlsx(df, location)
        self.save_dfs_to_xlsx(df_locs)


def main(args: argparse.Namespace):
    if args.traverse:
        traverser = SwiggyTraverser(skip_exists=False, date_str=args.date)
        traverser.run()

    if args.summarize:
        summarizer = SwiggySummarizer(date_str=args.date)
        summarizer.run()


if __name__ == "__main__":
    arg_parser = TraverserArgParser()
    args = arg_parser.parse_args()
    with Runtimer():
        main(args)

    # Case 1: traverse, scrape, save
    # python -m web.swiggy.traverser -s

    # Case 2: summarize
    # python -m web.swiggy.traverser -e
