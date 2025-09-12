import argparse
import sys

from acto import Perioder
from tclogger import shell_cmd

from configs.envs import LOGS_ROOT, WEBSITE_NAMES, WEBSITE_LITERAL


class ScrapeBatcherAction:
    def __init__(self, website: WEBSITE_LITERAL):
        self.pattern = "****-**-** 13:00:00"
        log_name = f"action_scrape_batcher_{website}.log"
        self.perioder = Perioder(self.pattern, log_path=LOGS_ROOT / log_name)
        self.env = "DISPLAY=:99 DBUS_SESSION_BUS_ADDRESS=none"
        self.cmd_scrape = f"{self.env} python -m web.{website}.batcher -s"

    def desc_func(self, run_dt_str: str):
        self.func_strs = [self.cmd_scrape]
        self.desc_str = "\n".join(self.func_strs)
        return self.func_strs, self.desc_str

    def func(self):
        for func_str in self.func_strs:
            shell_cmd(func_str)

    def run(self):
        self.perioder.bind(self.func, desc_func=self.desc_func)
        self.perioder.run()


class ExtractBatcherAction:
    def __init__(self):
        self.pattern = "****-**-** 15:45:00"
        self.perioder = Perioder(
            self.pattern, log_path=LOGS_ROOT / f"action_extract_batcher.log"
        )
        # swiggy must run last, as it uses data of blinkit and zepto
        websites = ["blinkit", "zepto", "swiggy", "dmart"]
        self.cmds_extract = [
            f"python -m web.{website}.batcher -e" for website in websites
        ]
        self.cmds_file = [
            "python -m file.excel_merger -m -k",
            "python -m file.email",
        ]

    def desc_func(self, run_dt_str: str):
        self.func_strs = [*self.cmds_extract, *self.cmds_file]
        self.desc_str = "\n".join(self.func_strs)
        return self.func_strs, self.desc_str

    def func(self):
        for func_str in self.func_strs:
            shell_cmd(func_str)

    def run(self):
        self.perioder.bind(self.func, desc_func=self.desc_func)
        self.perioder.run()


class BatcherActionArgParser(argparse.ArgumentParser):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.add_argument("-w", "--website", type=str, choices=WEBSITE_NAMES)
        self.add_argument("-s", "--scrape", action="store_true")
        self.add_argument("-e", "--extract", action="store_true")

    def parse_args(self):
        self.args, self.unknown_args = self.parse_known_args(sys.argv[1:])
        return self.args


def main(args: argparse.Namespace):
    if args.scrape:
        scrape_action = ScrapeBatcherAction(website=args.website)
        scrape_action.run()

    if args.extract:
        extract_action = ExtractBatcherAction()
        extract_action.run()


if __name__ == "__main__":
    arg_parser = BatcherActionArgParser()
    args = arg_parser.parse_args()
    main(args)

    # Case 1: batcher action of scrape
    # python -m cli.action -s -w blinkit
    # python -m cli.action -s -w swiggy
    # python -m cli.action -s -w zepto
    # python -m cli.action -s -w dmart

    # Case 2: batcher action of extract, merge, and email
    # python -m cli.action -e
