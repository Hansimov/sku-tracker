import argparse
import json
import pandas as pd
import re

from acto import Retrier
from dataclasses import dataclass
from DrissionPage._pages.chromium_tab import ChromiumTab
from pathlib import Path
from time import sleep
from tclogger import logger, logstr, brk, get_now_str, Runtimer, dict_get
from tclogger import raise_breakpoint
from urllib.parse import parse_qs, urlparse, urlencode, quote, unquote

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


def get_url_params(url: str) -> dict:
    return parse_qs(urlparse(url).query)


def get_url_param_value(url: str, key: str) -> str:
    params = get_url_params(url)
    values = params.get(key, [])
    if values:
        return values[0]
    return None


def urlencode_quote(params: dict) -> str:
    return urlencode(params, quote_via=quote)


class SwiggyCategoriesExtractor:
    def __init__(self, client: BrowserClient, verbose: bool = False):
        self.client = client
        self.verbose = verbose

    def extract_main_info_from_card(self, card: dict) -> dict:
        main_info = {
            "id": dict_get(card, "id", None),
            "name": dict_get(card, "header.title", "").strip(),
        }
        logger.mesg(main_info, indent=4)
        return main_info

    def unify_link(self, link: str) -> str:
        if not link:
            return link
        link = link.replace("swiggy://ageConsent?url=", "")
        link = unquote(link)
        link = link.replace("swiggy://stores", SWIGGY_URL)
        link = link.replace(" ", "+")
        return link

    def extract_items_info_from_card(self, card: dict) -> list[dict]:
        items_info = []
        items = dict_get(card, "gridElements.infoWithStyle.info", [])
        for item in items:
            link = dict_get(item, "action.link", None)
            link = self.unify_link(link)
            item_info = {
                "id": dict_get(item, "id", None),
                "name": dict_get(item, "description", "").strip(),
                "link": link,
                # "widgetId": dict_get(item, "analytics.extraFields.widgetId", None),
            }
            items_info.append(item_info)
        return items_info

    def should_skip_main(self, main_info: dict):
        name: str = dict_get(main_info, "name", "").lower()
        for prefix in ["best from noice", "shop by store"]:
            if name.startswith(prefix):
                return True
        return False

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
        self,
        client: BrowserClient,
        switcher: SwiggyLocationSwitcher,
        date_str: str = None,
        location: str = None,
    ):
        self.client = client
        self.switcher = switcher
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

    def run(self, location_idx: int = None):
        if not self.switcher.is_at_idx(location_idx):
            self.switcher.set_location(location_idx)
        resp_data = self.fetch()
        if resp_data:
            if resp_data:
                self.dump(resp_data)
        else:
            return {}


class SwiggyFiltersExtractor:
    def extract(self, resp: dict, listing_params: dict = {}, cname: str = None) -> dict:
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
        if not filters:
            logger.warn("  × No filters found in response")
            raise_breakpoint()
            return {}

        categ_info = {
            "categ_id": dict_get(resp, "data.selectedCategoryId", None),
            "categ_name": dict_get(resp, "data.selectedCategoryName", None),
            "cname": cname,
        }
        for filter_dict in filters:
            link_params = {
                "categoryName": dict_get(categ_info, "categ_name", None),
                "custom_back": True,
                "filterId": dict_get(filter_dict, "id", None),
                "offset": 0,
                "showAgeConsent": False,
                "storeId": "1135722",
                "taxonomyType": dict_get(listing_params, "taxonomyType", None),
            }
            link = f"{SWIGGY_LISTING_URL}?{urlencode_quote(link_params)}"
            filter_item = {
                "id": dict_get(filter_dict, "id", None),
                "name": dict_get(filter_dict, "name", "").strip(),
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
        filters_count = len(dict_get(categ_filters, "filters", []))
        categ_name = dict_get(categ_filters, "categ_name", None)
        if not categ_name:
            logger.warn("  × No categ_name found, skip saving")
            return
        logger.okay(f"  ✓ Save {logstr.mesg(filters_count)} filters to:", end=" ")
        if not save_path.exists():
            save_path.parent.mkdir(parents=True, exist_ok=True)
            data = {}
        else:
            data = load_json(save_path)
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
                widget_text = re.sub("<.*?>", "", widget_text)
                logger.note(f"    * {widget_text}")
                continue
            if widget_type.lower() == "product_list":
                widgets_data = dict_get(widget, "data", [])
        has_more = dict_get(resp, "data.hasMore", True)
        if has_more and not widgets_data:
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
        return logstr.note(f"{self.sname} ({self.cid}/{self.sid})")

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
        return logstr.note(f"{self.cname} ({self.cid})")

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
        return None

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
        sname_data = dict_get(data, sctx.sname, {})
        filters = dict_get(sname_data, "filters", [])
        if filters:
            return sname_data
        return None

    def scrape_filters(
        self, sctx: SwiggySubCategoryContext, skip_exists: bool = True
    ) -> dict:
        # https://www.swiggy.com/api/instamart/category-listing?categoryName=Fresh%20Fruits&storeId=1135722&pageNo=0&offset=0&filterName=&primaryStoreId=1135722&secondaryStoreId=1396282&taxonomyType=Speciality%20taxonomy%201
        if skip_exists:
            local_filters_dict = self.load_local_filters(sctx)
            if local_filters_dict:
                filters_count = len(local_filters_dict.get("filters", []))
                logger.okay(
                    f"  ✓ Load {logstr.mesg(filters_count)} local filters: "
                    f"{logstr.mesg(brk(sctx.cname))} - {logstr.file(brk(sctx.sname))}"
                )
                return local_filters_dict
        filters_path = get_filters_dump_path(self.date_str, self.location)
        tab = self.client.browser.latest_tab
        taxonomyType = get_url_param_value(sctx.url, "taxonomyType")
        listing_params = {
            "categoryName": sctx.sname,
            "storeId": "1135722",
            "primaryStoreId": "1135722",
            "secondaryStoreId": "1396282",
            "taxonomyType": taxonomyType,
        }
        url = f"{SWIGGY_API_LISTING_URL}?{urlencode_quote(listing_params)}"
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

        if resp_json and dict_get(resp_json, "data"):
            categ_filters = self.filters_extractor.extract(
                resp_json, listing_params=listing_params, cname=sctx.cname
            )
            self.filters_extractor.save(categ_filters, filters_path)
        else:
            logger.warn(f"  × No filters from url:")
            logger.file(f"    *  api url: {url}")
            logger.file(f"    * sctx.url: {sctx.url}")
            raise_breakpoint()
            return {}
        return categ_filters

    def get_listings_path(
        self, sctx: SwiggySubCategoryContext, filter_item: dict
    ) -> Path:
        listings_root = get_filters_dump_path(self.date_str, self.location).parent
        filter_name = dict_get(filter_item, "name")
        path_parts = [sctx.cname, sctx.sname, f"{filter_name}.json"]
        listings_path = listings_root.joinpath(*path_parts)
        return listings_path

    def load_local_listings(
        self, sctx: SwiggySubCategoryContext, filter_item: dict
    ) -> dict:
        listings_path = self.get_listings_path(sctx, filter_item)
        if not listings_path.exists():
            return None
        data = load_json(listings_path)
        listings = dict_get(data, "listings", [])
        if listings:
            return data
        return []

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
        taxonomy_type = dict_get(filter_item, "type", None)
        listing_params = {
            "filterId": dict_get(filter_item, "id", None),
            "storeId": "1135722",
            "primaryStoreId": "1135722",
            "secondaryStoreId": "1396282",
            "type": taxonomy_type,
            "categoryName": sctx.sname,
            "filterName": filter_name,
        }
        listing_params.update({"pageNo": page_no, "limit": limit, "offset": offset})
        url = f"{SWIGGY_API_FILTER_URL}?{urlencode_quote(listing_params)}"
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
        self,
        sctx: SwiggySubCategoryContext,
        filter_item: dict,
        skip_exists: bool = True,
    ) -> list[dict]:
        if skip_exists:
            local_listings_dict = self.load_local_listings(sctx, filter_item)
            if local_listings_dict:
                listings_count = len(dict_get(local_listings_dict, "listings", []))
                logger.okay(
                    f"  ✓ Load {logstr.mesg(listings_count)} local listings: "
                    f"{logstr.mesg(brk(sctx.cname))} - {logstr.file(brk(sctx.sname))} - "
                    f"{logstr.file(brk(dict_get(filter_item, 'name', None)))}"
                )
                return local_listings_dict

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
        listings_data = []
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
                listings = self.listing_extractor.extract(listing_resp)
                listings_data.extend(listings)
                has_more = dict_get(listing_resp, "data.hasMore", False)
                has_more_str = logstr.mesg(f"hasMore={has_more}")
                logger.okay(
                    f"    ✓ Extract {logstr.file(len(listings))} products, {has_more_str}"
                )
            else:
                logger.warn(f"    × Failed to fetch page {page_no}")
                has_more = False

            if has_more:
                page_no += 1
                offset += limit
                sleep(2)
            logger.restore_indent()

        logger.okay(
            f"  ✓ Fetched {logstr.mesg(len(listings_data))} products of "
            f"{logstr.file(brk(filter_name))}"
        )

        self.save_listings(listings_data, sctx, filter_item)

        return listings_data

    def construct_listings_save_data(
        self,
        sctx: SwiggySubCategoryContext,
        listings_data: list[dict],
        filter_item: dict,
    ) -> dict:
        save_data = {
            "categ": sctx.cname,
            "sub_categ": sctx.sname,
            "cid": sctx.cid,
            "sid": sctx.sid,
            "url": sctx.url,
            "filter_id": dict_get(filter_item, "id", None),
            "filter_name": dict_get(filter_item, "name", None),
            "filter_link": dict_get(filter_item, "link", None),
            "location": self.location,
            "count": len(listings_data),
            "count_expected": dict_get(filter_item, "productCount", None),
            "listings": listings_data,
        }
        return save_data

    def save_listings(
        self,
        listings_data: list[dict],
        sctx: SwiggySubCategoryContext,
        filter_item: dict,
    ):
        listings_path = self.get_listings_path(sctx, filter_item)
        logger.okay(f"  ✓ Save {logstr.mesg(len(listings_data))} items to:", end=" ")
        listings_path.parent.mkdir(parents=True, exist_ok=True)
        save_data = self.construct_listings_save_data(
            sctx=sctx, listings_data=listings_data, filter_item=filter_item
        )
        with open(listings_path, "w", encoding="utf-8") as wf:
            json.dump(save_data, wf, ensure_ascii=False, indent=4)
        logger.okay(f"{brk(listings_path)}")

    def process_context(
        self, cctx: SwiggyCategoryContext, sctx: SwiggySubCategoryContext
    ):
        with logger.temp_indent(2):
            categ_filters = self.scrape_filters(sctx)
            for filter_item in dict_get(categ_filters, "filters", []):
                self.scrape_listings(sctx, filter_item, skip_exists=True)

    def wait_next(self, seconds: int = 8):
        logger.note(f"  > Waiting {seconds}s for next ...")
        sleep(seconds)

    def run(self, location_idx: int = 0):
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
            client=self.client, switcher=self.switcher, date_str=self.date_str
        )
        self.scraper = SwiggyCategoryScraper(
            client=self.client, switcher=self.switcher, date_str=self.date_str
        )

    def run(self):
        for location_idx, location_item in enumerate(self.locations[:]):
            location_name = location_item.get("name", "")
            location_text = location_item.get("text", "")
            self.fetcher.location = location_name
            self.scraper.location = location_name
            logger.hint(f"> Location: {location_name} - {location_text}")
            categ_path = get_categ_dump_path(
                date_str=self.date_str, location=location_name
            )
            if self.skip_exists and categ_path.exists():
                logger.mesg(
                    f"> Skip fetch existed categories: {logstr.file(brk(categ_path))}"
                )
            else:
                self.fetcher.run(location_idx=location_idx)
            self.scraper.run(location_idx=location_idx)


CATEG_COLUMNS = ["categ", "sub_categ", "filter_name", "filter_link"]
PRODUCT_COLUMNS = [
    "product_name",
    "brand",
    "product_id",
    "price",
    "mrp",
    "unit",
    "quantity",
    "in_stock",
]
DF_INT_COLUMNS = ["price", "mrp", "quantity"]
COLUMN_RENAMES = {}


class SwiggySummarizer:
    def __init__(self, date_str: str = None, locations: list = None):
        self.date_str = norm_date_str(date_str)
        self.locations = locations or SWIGGY_LOCATIONS
        self.summary_root = get_summary_root(self.date_str)

    def get_listings_path(
        self, sctx: SwiggySubCategoryContext, filter_item: dict, location: str
    ) -> Path:
        listings_root = get_filters_dump_path(self.date_str, location).parent
        filter_name = dict_get(filter_item, "name")
        path_parts = [sctx.cname, sctx.sname, f"{filter_name}.json"]
        listings_path = listings_root.joinpath(*path_parts)
        return listings_path

    def categ_dict_to_row(self, categ_data: dict) -> dict:
        return {col: categ_data.get(col, None) for col in CATEG_COLUMNS}

    def product_dict_to_row(self, product: dict) -> dict:
        product_id = product.get("product_id", None)
        product_name = product.get("product_name", None)
        if not product_id and not product_name:
            return {}
        row = {col: product.get(col, None) for col in PRODUCT_COLUMNS}
        if product_id and product_name:
            product_link = f"{SWIGGY_CATEG_URL}/item/{product_id}"
        else:
            product_link = None
        row["product_link"] = product_link
        return row

    def get_rows_from_context(
        self, sctx: SwiggySubCategoryContext, location: str
    ) -> list[dict]:
        filters_path = get_filters_dump_path(self.date_str, location)
        if not filters_path.exists():
            logger.warn(f"  × Filters not exists: {logstr.file(brk(filters_path))}")
            return []
        res = []
        filters_data = load_json(filters_path)
        filter_items = dict_get(filters_data, sctx.sname, {}).get("filters", [])
        for filter_item in filter_items:
            listings_path = self.get_listings_path(
                sctx, filter_item=filter_item, location=location
            )
            if not listings_path.exists():
                logger.warn(
                    f"  × Listings not exists: {logstr.file(brk(listings_path))}"
                )
                continue
            listings_data = load_json(listings_path)
            categ_row = self.categ_dict_to_row(listings_data)
            products = dict_get(listings_data, "listings", []) or []
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
            res.extend(rows)
        return res

    def rows_to_df(self, rows: list[dict]) -> pd.DataFrame:
        df = pd.DataFrame(rows)
        for col in DF_INT_COLUMNS:
            if col not in df.columns:
                continue
            df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int64")
        if not df.empty:
            df = df.drop_duplicates(ignore_index=True)
        if COLUMN_RENAMES:
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


def run_traverser(args: argparse.Namespace):
    try:
        traverser = SwiggyTraverser(skip_exists=True, date_str=args.date)
        traverser.run()
    except Exception as e:
        logger.warn(e)
        logger.warn(f"> Closing tabs ...")
        sleep(5)
        traverser.client.close_other_tabs()
        raise e


def main(args: argparse.Namespace):
    if args.traverse:
        with Retrier(max_retries=50, retry_interval=60) as retrier:
            retrier.run(run_traverser, args=args)

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
