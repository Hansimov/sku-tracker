import json
import pandas as pd
import re

from dataclasses import dataclass
from DrissionPage._pages.chromium_tab import ChromiumTab
from pathlib import Path
from time import sleep
from tclogger import logger, logstr, brk, get_now_str
from tclogger import dict_get
from typing import Literal

from configs.envs import DATA_ROOT, BLINKIT_LOCATIONS, BLINKIT_TRAVERSER_SETTING
from web.blinkit.scraper import BlinkitLocationChecker, BlinkitLocationSwitcher
from web.browser import BrowserClient
from web.fetch import fetch_with_retry
from web.constants import norm_date_str

WEBSITE_NAME = "blinkit"
BLINKIT_CATEG_URL = "https://blinkit.com/categories"
BLINKIT_FLAG_URL = "https://blinkit.com/api/feature-flags/receive"
BLINKIT_CATEG_JS = "https://blinkit.com/.*/categories.*js"
BLINKIT_DEEPLINK_URL = "https://blinkit.com/v2/search/deeplink"
BLINKIT_LISTING_URL = "https://blinkit.com/v1/layout/listing_widgets"
# -/(\.(js|woff|css|svg|ico|png)|data:)/


def get_dump_root(date_str: str = None) -> Path:
    return DATA_ROOT / "traverses" / norm_date_str(date_str) / WEBSITE_NAME


def get_categ_dump_path(date_str: str = None) -> Path:
    return get_dump_root(date_str) / "categories.json"


def load_json(json_path: Path) -> dict:
    if not json_path.exists():
        return {}
    with open(json_path, "r", encoding="utf-8") as rf:
        return json.load(rf)


def raise_breakpoint():
    raise NotImplementedError("× In Develop Mode")


class BlinkitCategoriesExtractor:
    def __init__(self, client: BrowserClient, verbose: bool = False):
        self.client = client
        self.verbose = verbose

    def js_to_json_str(self, js_str: str) -> str:
        """Convert JS object notation to JSON string"""
        tab = self.client.browser.latest_tab
        # JSON.stringify()
        json_str = tab.run_js(
            f"""JSON.stringify({js_str});""",
            as_expr=True,
        )
        return json_str

    def extract_categories_from_js_str(self, js_str: str) -> list:
        """Extract a.CATEGORY data from JavaScript content"""
        if not js_str:
            logger.warn("  × Empty js str")
            return []
        pattern = r"a\.CATEGORY\s*=\s*(\[.*?\])},\s*66557"
        match = re.search(pattern, js_str, re.DOTALL)
        if not match:
            logger.warn("  × Not find `a.CATEGORY` pattern")
            return []

        categories = []
        match_str = match.group(1)
        logger.okay(f"  + Found `a.CATEGORY` pattern in js")
        try:
            json_str = self.js_to_json_str(match_str)
            categories = json.loads(json_str)
            logger.okay(f"  + Parsed {len(categories)} categories")
        except Exception as e:
            logger.warn(f"  × Failed to parse categories from js: {e}")
            return []

        return categories

    def extract(self, js_str: str) -> dict:
        logger.enter_quiet(not self.verbose)
        if not js_str:
            logger.warn("  × Empty response js to extract")
            logger.exit_quiet(not self.verbose)
            return {}
        categ_data = {}
        categories = self.extract_categories_from_js_str(js_str)
        if categories:
            categ_data = {"categories": categories, "count": len(categories)}
        else:
            categ_data = {"categories": [], "count": -1}
        logger.exit_quiet(not self.verbose)
        return categ_data


class BlinkitCategoriesFetcher:
    def __init__(self, client: BrowserClient, date_str: str = None):
        self.client = client
        self.date_str = norm_date_str(date_str)
        self.extractor = BlinkitCategoriesExtractor(client=client, verbose=True)
        self.dump_path = get_categ_dump_path(self.date_str)

    def get_cookies(self, tab: ChromiumTab) -> dict:
        cookies_dict = tab.cookies(all_info=True).as_dict()
        cookies_dict["url"] = tab.url
        cookies_dict["now"] = get_now_str()
        return cookies_dict

    def fetch(self) -> dict:
        categ_url = BLINKIT_CATEG_URL
        logger.note(f"> Visiting categories url:")
        logger.file(f"  * {categ_url}")

        self.client.start_client()
        tab = self.client.browser.latest_tab
        tab.set.load_mode.none()

        listen_targets = [BLINKIT_FLAG_URL, BLINKIT_CATEG_JS]
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
            if packet_url == BLINKIT_FLAG_URL:
                logger.okay(f"  + Flags packet captured: {packet_url_str}")
            elif re.match(BLINKIT_CATEG_JS, packet_url):
                logger.okay(f"  + Categories JS packet captured: {packet_url_str}")
                tab.stop_loading()
                categ_packet = packet
                break
            else:
                logger.warn(f"  × Unexpected packet: {packet_url_str}")

        if categ_packet:
            categ_resp = categ_packet.response
            if categ_resp:
                categ_js_str = categ_resp.body
                categ_data = self.extractor.extract(categ_js_str)

        if categ_data:
            categ_data["cookies"] = self.get_cookies(tab)

        self.client.stop_client(close_browser=False)
        return categ_data

    def dump(self, resp: dict):
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


class BlinkitListingExtractor:
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


class BlinkitListingScroller:
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
            return False
        if isinstance(js_res, dict) and js_res.get("ok"):
            return True
        else:
            logger.warn(f"  × Scroll JS result: {js_res}")
            return False


@dataclass
class BlinkitSubCategoryContext:
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
class BlinkitCategoryContext:
    cidx: int
    ctotal: int
    cname: str
    cid: int
    data: dict
    sctxs: list[BlinkitSubCategoryContext]

    def idx_str(self) -> str:
        return f"[{logstr.file(self.cidx)}/{logstr.mesg(self.ctotal)}]"

    def label_str(self) -> str:
        return logstr.note(f"{self.cname} (cid/{self.cid})")

    def idx_label_str(self) -> str:
        return f"{self.idx_str()} {self.label_str()}"

    def log_info(self):
        logger.note(f"> {self.idx_label_str()}")


class BlinkitCategoryIterator:
    def __init__(self, date_str: str = None, location: str = None):
        self.date_str = norm_date_str(date_str)
        self.location = location
        self.dump_root = get_dump_root(self.date_str)
        self.load_categories()

    def load_categories(self) -> list[dict]:
        self.categ_path = get_categ_dump_path(self.date_str)
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
        mark = re.sub(r"[^a-zA-Z0-9]+", "-", name.lower()).strip("-")
        return f"https://blinkit.com/cn/{mark}/cid/{cid}/{sid}"

    def __iter__(self):
        ctotal = len(self.categories)
        for cidx, categ in enumerate(self.categories, start=1):
            cname = categ.get("name", "")
            cid = categ.get("id", -1)
            sub_categs = categ.get("subCategories", []) or []
            stotal = len(sub_categs)
            sctxs: list[BlinkitSubCategoryContext] = []
            for sidx, sub_categ in enumerate(sub_categs, start=1):
                sname = sub_categ.get("name", "")
                sid = sub_categ.get("id", -1)
                url = self.get_sub_categ_url(sname, cid, sid)
                json_path = self.get_json_path(cid=cid, sid=sid)
                sctxs.append(
                    BlinkitSubCategoryContext(
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
            yield BlinkitCategoryContext(
                cidx=cidx,
                ctotal=ctotal,
                cname=cname,
                cid=cid,
                data=categ,
                sctxs=sctxs,
            )


class BlinkitCategoryScraper:
    def __init__(self, client: BrowserClient, date_str: str = None):
        self.client = client
        self.date_str = norm_date_str(date_str)
        self.extractor = BlinkitListingExtractor()
        self.scroller = BlinkitListingScroller()

    def scrape(self, url: str) -> list:
        tab = self.client.browser.latest_tab
        listen_targets = [BLINKIT_LISTING_URL]
        tab.listen.start(targets=listen_targets)
        tab.set.load_mode.none()
        tab.get(url)

        logger.mesg(f"  ✓ Title: {brk(tab.title)}")
        logger.note(f"  > Listening targets:")
        for target in listen_targets:
            logger.file(f"    * {target}")

        products_data = []
        for packet in tab.listen.steps(timeout=30):
            packet_url = packet.url
            if len(packet_url) >= 70:
                packet_url_parts = packet_url.split("&")
                packet_url_str = "&".join(packet_url_parts[:2])
            else:
                packet_url_str = packet_url
            packet_url_str = logstr.file(brk(packet_url_str))
            if packet_url.startswith(BLINKIT_LISTING_URL):
                logger.okay(f"  + Listing packet captured: {packet_url_str}")
                resp = packet.response
                if resp:
                    resp_data = self.extractor.extract(resp.body)
                    item_count = len(resp_data)
                    logger.mesg(f"    * Extracted {item_count} items")
                    products_data.extend(resp_data)
                    if item_count < 15:
                        tab.stop_loading()
                        break
                    else:
                        scroll_res = self.scroller.scroll(tab)
                        if not scroll_res:
                            logger.warn("  × Unable to scroll listing container")
                            break
                        else:
                            sleep(3)
            else:
                logger.warn(f"  × Unexpected packet: {packet_url_str}")
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
            logger.warn(
                f"  ? Items count {logstr.mesg(items_count)} is 15x, may be incomplete"
            )
            return "incomplete"
        return "exists"

    def construct_save_data(
        self,
        cctx: BlinkitCategoryContext,
        sctx: BlinkitSubCategoryContext,
        products_data: list[dict],
    ) -> dict:
        save_data = {
            "url": sctx.url,
            "categ": cctx.cname,
            "sub_categ": sctx.sname,
            "cid": cctx.cid,
            "sid": sctx.sid,
            "count": len(products_data),
            "products": products_data,
        }
        return save_data

    def process_context(
        self, cctx: BlinkitCategoryContext, sctx: BlinkitSubCategoryContext
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

    def run(self, location: str = None):
        iterator = BlinkitCategoryIterator(date_str=self.date_str, location=location)
        self.client.start_client()
        for cctx in iterator:
            cctx.log_info()
            for sctx in cctx.sctxs:
                json_path = sctx.json_path
                json_status = self.check_json_status(json_path)
                if json_status == "exists":
                    # self.skip_json(json_path)
                    continue
                sctx.log_info()
                if json_status == "incomplete":
                    raise_breakpoint()
                    continue
                # not_exists/incomplete: scrape and save
                self.process_context(cctx=cctx, sctx=sctx)
                self.wait_next(8)
                # raise_breakpoint()
        self.client.stop_client()


class BlinkitTraverser:
    def __init__(
        self,
        skip_exists: bool = True,
        date_str: str = None,
        client_settings: dict = None,
        locations: list = None,
    ):
        self.skip_exists = skip_exists
        self.date_str = norm_date_str(date_str)
        self.client_settings = client_settings or BLINKIT_TRAVERSER_SETTING
        self.locations = locations or BLINKIT_LOCATIONS
        self.client = BrowserClient(**self.client_settings)
        self.checker = BlinkitLocationChecker(locations=self.locations)
        self.switcher = BlinkitLocationSwitcher(
            client_settings=self.client_settings, locations=self.locations
        )
        self.fetcher = BlinkitCategoriesFetcher(
            client=self.client, date_str=self.date_str
        )
        self.scraper = BlinkitCategoryScraper(
            client=self.client, date_str=self.date_str
        )

    def run(self):
        for location_idx, location_item in enumerate(self.locations[:1]):
            location_name = location_item.get("name", "")
            location_text = location_item.get("text", "")
            is_set_location = False
            categ_path = self.fetcher.dump_path
            if self.skip_exists and categ_path.exists():
                logger.mesg(
                    f"> Skip fetch existed categories: {logstr.file(brk(categ_path))}"
                )
            else:
                if not is_set_location:
                    logger.hint(f"> New Location: {location_name} ({location_text})")
                    self.switcher.set_location(location_idx)
                    is_set_location = True
                self.fetcher.run()
            self.scraper.run(location=location_name)


CATEG_COLUMNS = ["url", "categ", "sub_categ", "cid", "sid"]
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


class BlinkitSummarizer:
    def __init__(
        self,
        date_str: str = None,
        locations: list = None,
    ):
        self.date_str = norm_date_str(date_str)
        self.locations = locations or BLINKIT_LOCATIONS

    def product_dict_to_row(self, product: dict) -> dict:
        if product.get("product_id") is None and product.get("product_name") is None:
            return {}
        return {col: product.get(col, None) for col in PRODUCT_COLUMNS}

    def categ_dict_to_row(self, data: dict) -> dict:
        return {col: data.get(col, None) for col in CATEG_COLUMNS}

    def run(self):
        for location_idx, location_item in enumerate(self.locations[:1]):
            location = location_item.get("name", "")
            iterator = BlinkitCategoryIterator(
                date_str=self.date_str, location=location
            )
            rows: list[dict] = []
            for cctx in iterator:
                cctx.log_info()
                for sctx in cctx.sctxs:
                    json_data = load_json(sctx.json_path)
                    categ_row = self.categ_dict_to_row(json_data)
                    products = dict_get(json_data, "products", []) or []
                    product_rows = [
                        self.product_dict_to_row(product) for product in products
                    ]
                    df_rows = []
                    for product_row in product_rows:
                        if not product_row:
                            continue
                        df_row = {
                            "date": self.date_str,
                            "location": location,
                            **categ_row,
                            **product_row,
                        }
                        df_rows.append(df_row)
                    rows.extend(df_rows)
            df = pd.DataFrame(rows)
            for col in DF_INT_COLUMNS:
                if col not in df.columns:
                    continue
                df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int64")
            print(df)


def test_traverser():
    traverser = BlinkitTraverser(skip_exists=True, date_str=None)
    traverser.run()


def test_summarizer():
    summarizer = BlinkitSummarizer(date_str=None)
    summarizer.run()


if __name__ == "__main__":
    test_traverser()
    # test_summarizer()

    # python -m web.blinkit.traverser
