import argparse
import sys

from tclogger import logger


class BatcherArgParser(argparse.ArgumentParser):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.add_argument("-s", "--scrape", action="store_true")
        self.add_argument("-e", "--extract", action="store_true")
        self.add_argument("-c", "--close-browser-after-done", action="store_true")
        self.add_argument("-f", "--force-scrape", action="store_true")
        self.add_argument("-d", "--date", type=str, default=None)

    def parse_args(self):
        self.args, self.unknown_args = self.parse_known_args(sys.argv[1:])
        self.check_args()
        return self.args

    def check_args(self, raise_error: bool = True) -> bool:
        if not (self.args.scrape or self.args.extract):
            err_mesg = "No valid argument: `-s` for scrape or `-e` for extract."
            logger.warn(err_mesg)
            if raise_error:
                raise ValueError(err_mesg)
            else:
                return False
        return True
