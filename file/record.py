import json

from tclogger import get_date_str

from configs.envs import WEBSITE_LITERAL, DATA_ROOT


class LinksRecorder:
    def __init__(
        self,
        website: WEBSITE_LITERAL,
        date_str: str = None,
    ):
        self.website = website
        self.date_str = get_date_str(date_str)
        self.init_paths()
        self.init_records()

    def init_paths(self):
        self.record_root = DATA_ROOT / "dumps" / self.date_str / self.website
        self.record_path = self.record_root / "records.json"

    def init_records(self):
        if not self.record_path.exists():
            self.record_path.parent.mkdir(parents=True, exist_ok=True)
            records = []
        else:
            with open(self.record_path, "r") as rf:
                records = json.load(rf)
        self.records = records

    def get_record(
        self, website: WEBSITE_LITERAL, location: str, link: str
    ) -> tuple[int, dict]:
        """
        [
            {
                "website": "swiggy",
                "location": "...",
                "link": "https://swiggy.com/product/...",
                "count": 2
            },
            ...
        ]
        """
        for idx, record in enumerate(self.records):
            if (
                record["website"] == website
                and record["location"] == location
                and record["link"] == link
            ):
                return idx, record
        return None, None

    def save_records(self):
        with open(self.record_path, "w") as wf:
            json.dump(self.records, wf, indent=4, ensure_ascii=False)

    def update_record(
        self,
        website: WEBSITE_LITERAL,
        location: str,
        link: str,
    ):
        idx, record = self.get_record(website, location, link)
        if idx is not None:
            self.records[idx]["count"] += 1
        else:
            self.records.append(
                {
                    "website": website,
                    "location": location,
                    "link": link,
                    "count": 1,
                }
            )
        self.save_records()

    def is_record_good(
        self, website: WEBSITE_LITERAL, location: str, link: str, max_count: int = 3
    ) -> bool:
        idx, record = self.get_record(website, location, link)
        if record is None:
            return True
        else:
            return record.get("count", 0) < max_count
