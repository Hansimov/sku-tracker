from acto import Perioder
from tclogger import shell_cmd

from configs.envs import LOGS_ROOT


class BatcherAction:
    def __init__(self):
        self.pattern = "****-**-** 11:00:00"
        self.perioder = Perioder(
            self.pattern, log_path=LOGS_ROOT / "action_batcher.log"
        )
        self.cmd_batcher = "./cli/run.sh"

    def desc_func(self, run_dt_str: str):
        self.func_strs = [self.cmd_batcher]
        self.desc_str = "\n".join(self.func_strs)
        return self.func_strs, self.desc_str

    def func(self):
        for func_str in self.func_strs:
            shell_cmd(func_str)

    def run(self):
        self.perioder.bind(self.func, desc_func=self.desc_func)
        self.perioder.run()


if __name__ == "__main__":
    action = BatcherAction()
    action.run()

    # python -m cli.action
