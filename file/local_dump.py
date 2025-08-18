import json

from pathlib import Path
from tclogger import logger, dict_get, match_val
from typing import Literal

from configs.envs import BLINKIT_LOCATIONS, SWIGGY_LOCATIONS
from configs.envs import ZEPTO_LOCATIONS, DMART_LOCATIONS
from configs.envs import WEBSITE_LITERAL


WEBSITE_DUMP_ADDRESS_KEYS_DICT = {
    "blinkit": "cookies.gr_1_locality",
    "swiggy": "userLocation.address",
    "zepto": "local_storage.state.userPosition.shortAddress",
    "dmart": "cookies.guest.preferredPIN",
}
WEBSITE_LOCATIONS_DICT = {
    "blinkit": BLINKIT_LOCATIONS,
    "swiggy": SWIGGY_LOCATIONS,
    "zepto": ZEPTO_LOCATIONS,
    "dmart": DMART_LOCATIONS,
}


def load_resp_from_dump_path(dump_path: Path) -> dict:
    if not dump_path.exists():
        return None
    with open(dump_path, "r") as rf:
        resp = json.load(rf)
    return resp


class LocalAddressExtractor:
    def __init__(self, website_name: WEBSITE_LITERAL):
        self.website_name = website_name
        self.address_keys = WEBSITE_DUMP_ADDRESS_KEYS_DICT.get(website_name, "")
        self.locations = WEBSITE_LOCATIONS_DICT.get(website_name, {})
        self.dump_addresses = [item.get("dump_address", "") for item in self.locations]

    def get_dump_address(self, resp: dict) -> str:
        dump_address = dict_get(resp, self.address_keys, None)
        return dump_address

    def map_dump_address_to_column_location(self, dump_address: str) -> str:
        _, closest_idx, _ = match_val(dump_address, self.dump_addresses, use_fuzz=True)
        column_location = self.locations[closest_idx].get("column_address", "")
        return column_location

    def map_dump_address_to_location_name(self, dump_address: str) -> str:
        _, closest_idx, _ = match_val(dump_address, self.dump_addresses, use_fuzz=True)
        location_name = self.locations[closest_idx].get("name", "")
        return location_name

    def get_column_location(self, resp: dict) -> str:
        dump_address = self.get_dump_address(resp)
        column_location = self.map_dump_address_to_column_location(dump_address)
        return column_location

    def get_location_name(self, resp: dict) -> str:
        dump_address = self.get_dump_address(resp)
        location_name = self.map_dump_address_to_location_name(dump_address)
        return location_name

    def check_dump_path_location(self, dump_path: Path, correct_location_name: str):
        resp = load_resp_from_dump_path(dump_path)
        if not resp:
            logger.warn(f"× No data of dump_path: {dump_path}")
            return False
        location_name = self.get_location_name(resp)
        if location_name != correct_location_name:
            logger.warn(f"× Location mismatch:")
            logger.mesg(f"  * local: {location_name}, correct: {correct_location_name}")
            logger.file(f"  * {dump_path}")
            return False
        else:
            return True


class SwiggyProductRespChecker:
    def check_product_resp(self, resp: dict) -> bool:
        item_data = dict_get(resp, "instamart.cachedProductItemData")
        if not item_data:
            return False
        return True

    def check(self, dump_path: Path) -> bool:
        resp = load_resp_from_dump_path(dump_path)
        if not resp:
            return False
        return self.check_product_resp(resp)


class DmartProductRespChecker:
    def check_product_resp(self, resp: dict) -> bool:
        item_data = dict_get(resp, "resp.pdpData.dynamicPDP.data.productData")
        if not item_data:
            return False
        return True

    def check(self, dump_path: Path) -> bool:
        resp = load_resp_from_dump_path(dump_path)
        if not resp:
            return False
        return self.check_product_resp(resp)
