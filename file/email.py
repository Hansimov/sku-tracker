import argparse
import sys

from acto import Emailer, EmailConfigsType, EmailContentType
from tclogger import logger, get_now_str, brk

from configs.envs import DATA_ROOT, EMAIL_SENDER, EMAIL_RECVER


class EmailSender:
    def __init__(
        self,
        configs: EmailConfigsType = EMAIL_SENDER,
        date_str: str = None,
        confirm_before_send: bool = False,
        verbose: bool = True,
    ):
        self.configs = configs
        self.date_str = date_str or get_now_str()[:10]
        self.confirm_before_send = confirm_before_send
        self.verbose = verbose
        self.init_paths()
        self.init_emailer()

    def check_output(self):
        if not self.output_merge_path.exists():
            err_mesg = f"× Report .xlsx not found: {brk(self.output_merge_path)}"
            logger.warn(err_mesg)
            raise FileNotFoundError(err_mesg)

    def init_paths(self):
        self.output_root = DATA_ROOT / "output" / self.date_str
        self.output_merge_path = self.output_root / f"sku_{self.date_str}.xlsx"
        self.check_output()

    def init_emailer(self):
        self.emailer = Emailer(
            self.configs,
            confirm_before_send=self.confirm_before_send,
            verbose=self.verbose,
        )

    def create_subject_and_body(self):
        username = self.configs.get("username", "")
        res = {
            "subject": f"[SKU] [{self.date_str}] Report",
            "body": f"File: {self.output_merge_path.name} <br/> From: <b>{username}</b> <br/> Sent: {get_now_str()}",
        }
        return res

    def send(self):
        subject_and_body = self.create_subject_and_body()
        content: EmailContentType = {
            "to": EMAIL_RECVER["to"],
            "cc": EMAIL_RECVER["cc"],
            **subject_and_body,
            "attachments": self.output_merge_path,
        }
        self.emailer.send(content)


class BatcherArgParser(argparse.ArgumentParser):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.add_argument("-d", "--date", type=str, default=None)
        self.add_argument("-c", "--confirm-before-send", action="store_true")

    def parse_args(self):
        self.args, self.unknown_args = self.parse_known_args(sys.argv[1:])
        return self.args


def main():
    parser = BatcherArgParser()
    args = parser.parse_args()
    email_sender = EmailSender(
        date_str=args.date, confirm_before_send=args.confirm_before_send
    )
    email_sender.send()


if __name__ == "__main__":
    main()

    # python -m file.email
    # python -m file.email -d 2025-07-21
    # python -m file.email -d 2025-07-21 -c
