import argparse
import json
import pandas as pd
import re

from dataclasses import dataclass
from DrissionPage._pages.chromium_tab import ChromiumTab
from pathlib import Path
from time import sleep
from tclogger import logger, logstr, brk, get_now_str, Runtimer, dict_get
from tclogger import raise_breakpoint
from typing import Literal
from urllib.parse import parse_qs, urlparse, urlencode, quote

from configs.envs import DATA_ROOT, SWIGGY_LOCATIONS, SWIGGY_TRAVERSER_SETTING
from web.swiggy.scraper import SwiggyLocationChecker, SwiggyLocationSwitcher
from web.blinkit.traverser import norm_name, load_json
from web.browser import BrowserClient
from web.constants import norm_date_str
from cli.arg import TraverserArgParser


WEBSITE_NAME = "swiggy"
SWIGGY_URL = "https://www.swiggy.com"
SWIGGY_CATEG_URL = "https://www.swiggy.com/instamart"
SWIGGY_LISTING_URL = "https://www.swiggy.com/instamart/category-listing"
SWIGGY_API_HOME_URL = "https://www.swiggy.com/api/instamart/home/v2\?.*"
SWIGGY_API_LISTING_URL = "https://www.swiggy.com/api/instamart/category-listing"
SWIGGY_API_FILTER_URL = "https://www.swiggy.com/api/instamart/category-listing/filter"
SWIGGY_API_FILTER_RE = (
    "https://www.swiggy.com/api/instamart/category-listing/filter\?.*"
)


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


def get_filters_dump_path(date_str: str = None, location: str = None) -> Path:
    dump_root = get_dump_root(date_str)
    path_parts = ["filters.json"]
    if location:
        path_parts = [location] + path_parts
    return dump_root.joinpath(*path_parts)


class SwiggyCategoriesExtractor:
    def __init__(self, client: BrowserClient, verbose: bool = False):
        self.client = client
        self.verbose = verbose

    def extract_main_info_from_card(self, card: dict) -> dict:
        main_info = {
            "id": dict_get(card, "id", None),
            "name": dict_get(card, "header.title", None),
        }
        logger.mesg(main_info, indent=4)
        return main_info

    def extract_items_info_from_card(self, card: dict) -> list[dict]:
        items_info = []
        items = dict_get(card, "gridElements.infoWithStyle.info", [])
        for item in items:
            link = dict_get(item, "action.link", None)
            if link:
                link = link.replace("swiggy://stores", SWIGGY_URL)
                link = link.replace(" ", "+")
            item_info = {
                "id": dict_get(item, "id", None),
                "name": dict_get(item, "description", None),
                "link": link,
                # "widgetId": dict_get(item, "analytics.extraFields.widgetId", None),
            }
            items_info.append(item_info)
        return items_info

    def should_skip_main(self, main_info: dict):
        return dict_get(main_info, "name", "").lower().startswith("best from noice")

    def extract_categories_from_json(self, categ_json: dict) -> list[dict]:
        cards = dict_get(categ_json, "data.cards", [])
        res = []
        for card in cards:
            card = dict_get(card, "card.card", {})
            main_info = self.extract_main_info_from_card(card)
            if self.should_skip_main(main_info):
                continue
            items_info = self.extract_items_info_from_card(card)
            card_info = {**main_info, "subCategories": items_info}
            res.append(card_info)
        return res

    def extract(self, categ_json: dict) -> dict:
        logger.enter_quiet(not self.verbose)
        if not categ_json:
            logger.warn("  × Empty response json to extract")
            logger.exit_quiet(not self.verbose)
            return {}
        categ_data = {}
        categories = self.extract_categories_from_json(categ_json)
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


class SwiggyFiltersExtractor:
    def extract(self, resp: dict) -> dict:
        """Return:
        ```json
        {
            "categ_id": "6822eeeded32000001e25aa1",
            "categ_name": "Fresh Vegetables",
            "filters": [
                {
                    "id": "6822eeeded32000001e25aa2",
                    "name": "Fresh Vegetables",
                    "type": "Speciality taxonomy 1",
                    "productCount": 87,
                    "link": "https://www.swiggy.com/instamart/category-listing?categoryName=Fresh+Vegetables&custom_back=True&filterId=6822eeeded32000001e25aa2&offset=0&showAgeConsent=False&storeId=1135722&taxonomyType=Speciality+taxonomy+1"
                },
                ...
            ]
        }
        ```
        """
        filter_items = []
        filters = dict_get(resp, "data.filters", []) or []
        categ_info = {
            "categ_id": dict_get(resp, "data.selectedCategoryId", None),
            "categ_name": dict_get(resp, "data.selectedCategoryName", None),
        }
        for filter_dict in filters:
            link_params = {
                "categoryName": dict_get(categ_info, "categ_name", None),
                "custom_back": True,
                "filterId": dict_get(filter_dict, "id", None),
                "offset": 0,
                "showAgeConsent": False,
                "storeId": "1135722",
                "taxonomyType": "Speciality taxonomy 1",
            }
            link = f"{SWIGGY_LISTING_URL}?{urlencode(link_params, quote_via=quote)}"
            filter_item = {
                "id": dict_get(filter_dict, "id", None),
                "name": dict_get(filter_dict, "name", None),
                "type": dict_get(filter_dict, "type", None),
                "productCount": dict_get(filter_dict, "productCount", None),
                "link": link,
            }
            filter_items.append(filter_item)
        res = {
            **categ_info,
            "filters": filter_items,
        }
        return res

    def save(self, categ_filters: dict, save_path: Path):
        logger.okay(f"  ✓ Save filters to:", end=" ")
        if not save_path.exists():
            save_path.parent.mkdir(parents=True, exist_ok=True)
            data = {}
        else:
            data = load_json(save_path)
        categ_name = dict_get(categ_filters, "categ_name", None)
        with open(save_path, "w", encoding="utf-8") as wf:
            data[categ_name] = categ_filters
            json.dump(data, wf, ensure_ascii=False, indent=4)
        logger.okay(f"{brk(save_path)}")


class SwiggyListingExtractor:
    def select_variation(self, variations: list[dict]) -> dict:
        if not variations:
            return {}
        if len(variations) == 1:
            return variations[0]
        for variation in variations:
            if dict_get(variation, "listing_variant", None):
                return variation
        return variations[0]

    def item_to_dict(self, item: dict) -> dict:
        variations = dict_get(item, "variations", []) or []
        variant = self.select_variation(variations) or {}
        item_info = {
            "product_id": dict_get(item, "product_id", None),
            # "product_id": dict_get(variant, "id", None),
            "product_name": dict_get(item, "display_name", None),
            "quantity": dict_get(variant, "cart_allowed_quantity.total", None),
            "price": dict_get(variant, "price.offer_price", None),
            "mrp": dict_get(variant, "price.mrp", None),
            "unit": dict_get(variant, "sku_quantity_with_combo", None),
            # "in_stock": dict_get(variant, "inventory.in_stock", None),
            "in_stock": dict_get(item, "in_stock", None),
            "brand": dict_get(variant, "brand", None),
            "sourced_from": dict_get(variant, "sourced_from", None),
            "super_category": dict_get(variant, "super_category", None),
        }
        return item_info

    def extract(self, resp: dict) -> list[dict]:
        res = []
        widgets = dict_get(resp, "data.widgets") or []
        widgets_data = []
        for widget in widgets:
            widget_type = dict_get(widget, "widgetInfo.widgetType", "")
            if widget_type.lower() == "text_widget":
                widget_text = dict_get(widget, "widgetInfo.title", "")
                logger.note(f"    * {widget_text}")
                continue
            if widget_type.lower() == "product_list":
                widgets_data = dict_get(widget, "data", [])
        if not widgets_data:
            logger.warn("    × No products data extracted")
            return []
        for item in widgets_data:
            item_dict = self.item_to_dict(item)
            res.append(item_dict)
        return res


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
        logger.note(f"  * {self.idx_label_str()}")


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
                url = sub_categ.get("link", None)
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
        self.listing_extractor = SwiggyListingExtractor()
        self.filters_extractor = SwiggyFiltersExtractor()

    def load_local_filters(self, sctx: SwiggySubCategoryContext) -> dict:
        filters_path = get_filters_dump_path(self.date_str, self.location)
        if not filters_path.exists():
            return None
        data = load_json(filters_path)
        if sctx.sname in data:
            return dict_get(data, sctx.sname, {})
        return None

    def scrape_filters(
        self, sctx: SwiggySubCategoryContext, skip_exists: bool = True
    ) -> dict:
        # https://www.swiggy.com/api/instamart/category-listing?categoryName=Fresh%20Fruits&storeId=1135722&pageNo=0&offset=0&filterName=&primaryStoreId=1135722&secondaryStoreId=1396282&taxonomyType=Speciality%20taxonomy%201
        if skip_exists:
            local_filters = self.load_local_filters(sctx)
            if local_filters:
                logger.okay(
                    f"  ✓ Load local filters: "
                    f"{logstr.mesg(brk(sctx.cname))} - {logstr.file(brk(sctx.sname))}"
                )
                return local_filters
        filters_path = get_filters_dump_path(self.date_str, self.location)
        tab = self.client.browser.latest_tab
        listing_params = {
            "categoryName": sctx.sname,
            "storeId": "1135722",
            "primaryStoreId": "1135722",
            "secondaryStoreId": "1396282",
            "taxonomyType": "Speciality taxonomy 1",
        }
        url = f"{SWIGGY_API_LISTING_URL}?{urlencode(listing_params, quote_via=quote)}"
        logger.note(f"  * GET filters: {logstr.mesg(brk(sctx.sname))}")
        tab.get(url, timeout=10)

        resp_json = None
        is_get_json = False
        while not is_get_json:
            try:
                resp_json = tab.json
                is_get_json = True
            except:
                sleep(1)

        if resp_json:
            categ_filters = self.filters_extractor.extract(resp_json)
            self.filters_extractor.save(categ_filters, filters_path)
        else:
            return {}
        return categ_filters

    def fetch_listing(
        self,
        tab: ChromiumTab,
        sctx: SwiggySubCategoryContext,
        filter_item: dict,
        page_no: int,
        offset: int,
        limit: int = 20,
    ) -> dict:
        # https://www.swiggy.com/api/instamart/category-listing/filter?filterId=6822eeeded32000001e25aa2&storeId=1135722&primaryStoreId=1135722&secondaryStoreId=1396282&type=Speciality%20taxonomy%201&pageNo=1&limit=20&filterName=Fresh%20Vegetables&categoryName=Fresh%20Vegetables&offset=20
        filter_name = dict_get(filter_item, "name", None)
        listing_params = {
            "filterId": dict_get(filter_item, "id", None),
            "storeId": "1135722",
            "primaryStoreId": "1135722",
            "secondaryStoreId": "1396282",
            "type": "Speciality taxonomy 1",
            "categoryName": sctx.sname,
            "filterName": filter_name,
        }
        listing_params.update({"pageNo": page_no, "limit": limit, "offset": offset})
        url = f"{SWIGGY_API_FILTER_URL}?{urlencode(listing_params, quote_via=quote)}"
        payload = {"facets": {}, "sortAttribute": ""}
        payload_json = json.dumps(payload)

        tab.listen.start(targets=SWIGGY_API_FILTER_RE, is_regex=True)

        fetch_js = f"""
        fetch("{url}", {{
            method: "POST",
            headers: {{
                "Content-Type": "application/json",
                "Accept": "application/json, text/plain, */*",
            }},
            body: '{payload_json}',
            credentials: "include"
        }});
        """
        tab.run_js(fetch_js)

        try:
            for packet in tab.listen.steps(timeout=15):
                if re.match(SWIGGY_API_FILTER_RE, packet.url):
                    packet_resp = packet.response
                    if packet_resp:
                        logger.okay(f"    ✓ Captured packet")
                        listing_resp = packet_resp.body
                        return listing_resp
                    else:
                        logger.warn(f"    × No response in packet")
        except Exception as e:
            logger.error(f"    × Error capturing packet: {e}")
        finally:
            tab.listen.stop()

        return None

    def scrape_listings(
        self, sctx: SwiggySubCategoryContext, filter_item: dict
    ) -> list[dict]:
        tab = self.client.browser.latest_tab
        if not tab.url.startswith(SWIGGY_LISTING_URL):
            logger.note(f"  * Visit: {logstr.file(sctx.url)}")
            tab.get(sctx.url, timeout=10)
            sleep(3)

        filter_name = dict_get(filter_item, "name", None)
        logger.note(
            f"  * GET listings: "
            f"{logstr.mesg(brk(sctx.sname))} - {logstr.file(brk(filter_name))}"
        )

        offset, page_no, limit = 0, 0, 20
        has_more = True
        res = []
        while has_more:
            logger.file(f"    * page={page_no}, offset={offset}")
            logger.store_indent()
            logger.indent(2)
            listing_resp = self.fetch_listing(
                tab=tab,
                sctx=sctx,
                filter_item=filter_item,
                page_no=page_no,
                offset=offset,
                limit=limit,
            )

            if listing_resp:
                products = self.listing_extractor.extract(listing_resp)
                res.extend(products)
                has_more = dict_get(listing_resp, "data.hasMore", False)
                logger.okay(
                    f"    ✓ Extract {logstr.file(len(products))} products,", end=" "
                )
                logger.mesg(f"hasMore={has_more}")
            else:
                logger.warn(f"    × Failed to fetch page {page_no}")
                has_more = False

            if has_more:
                page_no += 1
                offset += limit
                sleep(2)
            logger.restore_indent()

        logger.okay(
            f"  ✓ Fetched {logstr.mesg(len(res))} products of "
            f"{logstr.file(brk(filter_name))}"
        )
        return res

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
            categ_filters = self.scrape_filters(sctx)
            for filter_item in dict_get(categ_filters, "filters", []):
                self.scrape_listings(sctx, filter_item)
            raise_breakpoint()

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
                sctx.log_info()
                if not self.switcher.is_at_idx(location_idx):
                    self.switcher.set_location(location_idx)
                self.process_context(cctx=cctx, sctx=sctx)
                raise_breakpoint()
                self.wait_next(5)
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
            self.scraper.run(location=location_name, location_idx=location_idx)


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
        traverser = SwiggyTraverser(skip_exists=True, date_str=args.date)
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
