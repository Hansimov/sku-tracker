import argparse
import sys

from acto import Emailer, EmailConfigsType, EmailContentType
from datetime import timedelta
from pathlib import Path
from tclogger import logger, get_now_str, brk, str_to_t
from typing import Literal

from configs.envs import DATA_ROOT, EMAIL_SENDER, EMAIL_RECVER


class EmailSender:
    def __init__(
        self,
        configs: EmailConfigsType = EMAIL_SENDER,
        date_str: str = None,
        confirm_before_send: bool = False,
        task: Literal["daily", "weekly"] = "daily",
        verbose: bool = True,
    ):
        self.configs = configs
        self.date_str = date_str or get_now_str()[:10]
        self.confirm_before_send = confirm_before_send
        self.task = task
        self.verbose = verbose
        self.init_paths()
        self.init_emailer()

    def get_daily_report_path(self) -> Path:
        output_root = DATA_ROOT / "output" / self.date_str
        self.report_path = output_root / f"sku_{self.date_str}.xlsx"
        return self.report_path

    def get_weekly_report_path(self) -> Path:
        package_root = DATA_ROOT / "package"
        date_end = str_to_t(self.date_str)
        date_beg = date_end - timedelta(days=6)
        date_week = date_beg.isocalendar().week
        date_beg_str = date_beg.strftime("%Y%m%d")
        date_end_str = date_end.strftime("%Y%m%d")
        self.report_path = (
            package_root / f"sku_ww{date_week}_{date_beg_str}_{date_end_str}.xlsx"
        )
        self.date_beg_str = date_beg_str
        self.date_end_str = date_end_str
        self.date_week = date_week
        return self.report_path

    def check_output(self):
        if not self.report_path.exists():
            err_mesg = f"× Report .xlsx not found: {brk(self.report_path)}"
            logger.warn(err_mesg)
            raise FileNotFoundError(err_mesg)

    def init_paths(self):
        if self.task == "weekly":
            self.get_weekly_report_path()
        else:
            self.get_daily_report_path()

        self.check_output()

    def init_emailer(self):
        self.emailer = Emailer(
            self.configs,
            confirm_before_send=self.confirm_before_send,
            verbose=self.verbose,
        )

    def create_subject_and_body(self):
        username = self.configs.get("username", "")

        if self.task == "weekly":
            title = f"WW{self.date_week} ({self.date_beg_str} - {self.date_end_str})"
            subject_str = f"[SKU Weekly Summary] [{self.date_str}] {title}"
        else:
            subject_str = f"[SKU Daily Report] [{self.date_str}]"

        file_str = f"File: <b>{self.report_path.name}</b>"
        from_str = f"From: <b>{username}</b>"
        sent_str = f"Sent: {get_now_str()}"

        body_str = " <br/> ".join([file_str, from_str, sent_str])

        res = {
            "subject": subject_str,
            "body": body_str,
        }
        return res

    def send(self):
        subject_and_body = self.create_subject_and_body()
        content: EmailContentType = {
            "to": EMAIL_RECVER["to"],
            "cc": EMAIL_RECVER["cc"],
            **subject_and_body,
            "attachments": self.report_path,
        }
        self.emailer.send(content)


class BatcherArgParser(argparse.ArgumentParser):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.add_argument("-d", "--date", type=str, default=None)
        self.add_argument("-c", "--confirm-before-send", action="store_true")
        self.add_argument(
            "-t", "--task", type=str, choices=["daily", "weekly"], default="daily"
        )

    def parse_args(self):
        self.args, self.unknown_args = self.parse_known_args(sys.argv[1:])
        return self.args


def main():
    parser = BatcherArgParser()
    args = parser.parse_args()
    email_sender = EmailSender(
        date_str=args.date, confirm_before_send=args.confirm_before_send, task=args.task
    )
    email_sender.send()


if __name__ == "__main__":
    main()

    # Case 1: Send daily report
    # python -m file.email
    # python -m file.email -d 2025-07-21
    # python -m file.email -d 2025-07-21 -c

    # Case 2: Send weekly summary
    # python -m file.email -c -t weekly
